use std::path::{PathBuf, Path};
use std::collections::{HashSet};
use std::net::SocketAddr;
use std::sync::{Mutex, Arc};
use std::iter::FromIterator;
use std::sync::atomic::{Ordering, AtomicU64};
use std::ops::Deref;

use tokio::io::{AsyncReadExt, AsyncWriteExt, ErrorKind};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use serde::{Deserialize, Serialize};

use crate::files::{FileBlock, File, FileRequest, hash_file_block, ModifiedTime};
use crate::{files, sync};
use crate::sync::{SyncAction, FileChangesFinder, DeleteLog};
use crate::tracker::ClientId;
use crate::sync_engine::{FilesSyncStatus, FileBlockRequestDispatcher};
use crate::sync_clients::ClientsManager;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SyncCommand {
    RequestFileSync(u64),
    AcceptedFileSync(u64),
    SyncFiles(Vec<(String, ModifiedTime)>),
    GetFile(String, ModifiedTime, bool),
    GetFileDenied(String, ModifiedTime, bool),
    SendFile(String, ModifiedTime),
    StartSyncFile { filename: String, request: FileRequest, redistribute: bool },
    GetFileBlock { filename: String, block: FileBlock },
    FileBlock { filename: String, block: FileBlock, hash: String, content: Vec<u8> },
    DeleteFile(String, ModifiedTime)
}

impl SyncCommand {
    pub async fn receive_command<T: AsyncReadExt + Unpin>(socket: &mut T) -> tokio::io::Result<SyncCommand> {
        let num_bytes = socket.read_u64().await? as usize;
        let mut command_bytes = vec![0; num_bytes];
        socket.read_exact(&mut command_bytes[..]).await?;
        bincode::deserialize(&command_bytes).map_err(|_| tokio::io::Error::from(ErrorKind::Other))
    }

    pub async fn send_command<T: AsyncWriteExt + Unpin>(&self, socket: &mut T) -> tokio::io::Result<()> {
        let command_bytes = bincode::serialize(&self).unwrap();
        socket.write_u64(command_bytes.len() as u64).await?;
        socket.write_all(&command_bytes).await?;
        Ok(())
    }
}

impl std::fmt::Display for SyncCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncCommand::RequestFileSync(_) => write!(f, "RequestFileSync"),
            SyncCommand::AcceptedFileSync(_) => write!(f, "AcceptedFileSync"),
            SyncCommand::SyncFiles(_) => write!(f, "SyncFiles"),
            SyncCommand::GetFile(_, _, _) => write!(f, "GetFile"),
            SyncCommand::GetFileDenied(_, _, _) => write!(f, "GetFileDenied"),
            SyncCommand::SendFile(_, _) => write!(f, "SendFile"),
            SyncCommand::StartSyncFile { .. } => write!(f, "StartSyncFile"),
            SyncCommand::GetFileBlock { .. } => write!(f, "GetFileBlock"),
            SyncCommand::FileBlock { .. } => write!(f, "FileBlock"),
            SyncCommand::DeleteFile(_, _) => write!(f, "DeleteFile"),
        }
    }
}

pub type SyncCommandsSender = mpsc::UnboundedSender<SyncCommand>;
pub type SyncCommandsReceiver = mpsc::UnboundedReceiver<SyncCommand>;

#[derive(Copy, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChannelId(pub u64);

impl std::fmt::Display for ChannelId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Debug for ChannelId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct FileSyncManager {
    folder: PathBuf,
    clients_manager: ClientsManager,
    file_changes_finder: Mutex<FileChangesFinder>,
    delete_log: tokio::sync::Mutex<DeleteLog>,
    files_sync_status: Mutex<FilesSyncStatus>,
    file_block_request_dispatcher: FileBlockRequestDispatcher,
    next_sync_request_id: AtomicU64,
    sync_requests: Mutex<HashSet<u64>>
}

impl FileSyncManager {
    pub fn new(folder: PathBuf) -> FileSyncManager {
        FileSyncManager {
            folder: folder.clone(),
            clients_manager: ClientsManager::new(),
            file_changes_finder: Mutex::new(FileChangesFinder::new()),
            delete_log:  tokio::sync::Mutex::new(DeleteLog::new(folder.clone())),
            files_sync_status: Mutex::new(FilesSyncStatus::new()),
            file_block_request_dispatcher: FileBlockRequestDispatcher::new(),
            next_sync_request_id: AtomicU64::new(1),
            sync_requests: Mutex::new(HashSet::new())
        }
    }

    pub async fn run(&self,
                     client: TcpStream,
                     channel_id: ChannelId,
                     commands_channel: (SyncCommandsSender, SyncCommandsReceiver),
                     initial_sync: bool) -> tokio::io::Result<()> {
        let (mut commands_sender, mut commands_receiver) = commands_channel;
        let destination_address = client.peer_addr().unwrap();
        let (mut client_reader, mut client_writer) = client.into_split();

        tokio::spawn(async move {
            while let Some(command) = commands_receiver.recv().await {
                if let Err(_) = command.send_command(&mut client_writer).await {
                    break;
                }
            }
        });

        if initial_sync {
            self.request_sync(&mut commands_sender)?;
        }

        loop {
            let command = SyncCommand::receive_command(&mut client_reader).await?;
            self.handle(
                destination_address,
                channel_id,
                &mut commands_sender,
                command
            ).await?;
        }
    }

    async fn handle(&self,
                    destination_address: SocketAddr,
                    channel_id: ChannelId,
                    commands_sender: &mut SyncCommandsSender,
                    command: SyncCommand) -> tokio::io::Result<()> {
        match command {
            SyncCommand::RequestFileSync(id) => {
                if !self.files_sync_status.lock().unwrap().any_active() {
                    commands_sender.send(SyncCommand::AcceptedFileSync(id))
                        .map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
                } else {
                    println!("Denied sync request #{}", id);
                }
            }
            SyncCommand::AcceptedFileSync(id) => {
                println!("Got sync request #{}", id);

                let sync = self.sync_requests.lock().unwrap().insert(id);
                if sync {
                    println!("Requesting sync of files with {}.", destination_address);

                    let files = files::list_files(&self.folder).await?;

                    commands_sender.send(
                        SyncCommand::SyncFiles(
                            files
                                .iter()
                                .map(|file| (file.path.to_str().unwrap().to_owned(), file.modified))
                                .collect::<Vec<_>>()
                        )
                    ).map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;

                    self.next_sync();
                }
            }
            SyncCommand::SyncFiles(other_files) => {
                println!("Syncing files...");

                let files = files::list_files(&self.folder).await?;
                let other_files = other_files
                    .into_iter()
                    .map(|file| File::new(PathBuf::from(file.0), file.1))
                    .collect::<Vec<_>>();

                for action in sync::full_sync(files, other_files, &self.delete_log.lock().await.deref()) {
                    let command = match action {
                        SyncAction::GetFile(filename, modified) => {
                            println!("Get file: {}", filename);
                            SyncCommand::GetFile(filename, modified, true)
                        }
                        SyncAction::SendFile(filename, modified) => {
                            println!("Sending file: {}", filename);
                            SyncCommand::SendFile(filename, modified)
                        }
                        SyncAction::DeleteFile(filename, modified) => {
                            println!("Delete file: {}", filename);
                            SyncCommand::DeleteFile(filename, modified)
                        }
                    };

                    commands_sender.send(command)
                        .map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
                }
            }
            SyncCommand::GetFile(filename, modified, redistribute) => {
                if files::has_file(&self.folder.join(&filename), &modified).await {
                    commands_sender.send(files::start_file_sync(
                        &self.folder,
                        filename,
                        redistribute
                    ).await?).map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
                } else {
                    println!("Don't got file {} @ {}", filename, modified.to_unix_seconds());
                    commands_sender.send(SyncCommand::GetFileDenied(filename, modified, redistribute))
                        .map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
                }
            }
            SyncCommand::GetFileDenied(filename, modified, redistribute) => {
                self.get_file_from_random(filename, modified, redistribute, &HashSet::new())?;
            },
            SyncCommand::SendFile(filename, modified) => {
                self.get_file_from_random(filename, modified, false, &HashSet::new())?;
            },
            SyncCommand::StartSyncFile { filename, request, redistribute } => {
                if files::is_remote_newer(&self.folder.join(&filename), &request.modified).await {
                    if !self.files_sync_status.lock().unwrap().is_syncing(&filename) {
                        println!(
                            "Starting sync of file: {} ({} bytes), redistribute: {}",
                            filename,
                            request.size,
                            redistribute
                        );

                        let blocks = self.files_sync_status.lock().unwrap().add(
                            &self.folder,
                            &filename,
                            request,
                            redistribute
                        );

                        self.file_block_request_dispatcher.enqueue(
                            channel_id,
                            commands_sender.clone(),
                            filename,
                            blocks
                        );
                    } else {
                        println!("Sync request for file {} is already ongoing.", filename);
                    }
                } else {
                    println!("Sync request for file {} is older than what we have locally.", filename);
                }
            }
            SyncCommand::GetFileBlock { filename, block } => {
                commands_sender.send(files::send_file_block(
                    &self.folder,
                    filename,
                    &block
                ).await?).map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
            }
            SyncCommand::FileBlock { filename, block, hash, content } => {
                let actual_hash = hash_file_block(block.offset(), &content);

                if actual_hash == hash {
                    let result = self.files_sync_status.lock().unwrap().get_tmp_write_path(&filename);
                    if let Some((tmp_path, modified)) = result {
                        files::write_file_block(
                            &tmp_path,
                            &block,
                            modified,
                            &content,
                        ).await?;

                        self.received_file_block(channel_id, &filename, &block, content.len());
                        if self.try_complete_file_sync(&filename, modified).await? {
                            self.clients_manager.send_commands_all(SyncCommand::SendFile(filename, modified));
                        }
                    }
                } else {
                    println!(
                        "Receive error for file block: {}/{}: expected hash {} but got {}.",
                        filename,
                        block.number,
                        &hash[..10],
                        &actual_hash[..10]
                    );

                    commands_sender.send(SyncCommand::GetFileBlock {
                        filename,
                        block,
                    }).map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
                }
            }
            SyncCommand::DeleteFile(filename, modified) => {
                println!("Deleting file: {}", filename);
                if let Ok(()) = tokio::fs::remove_file(self.folder.join(Path::new(&filename))).await {
                    self.delete_log.lock().await.add_entry(&filename, modified);
                    self.file_changes_finder.lock().unwrap().add_external_delete(PathBuf::from(filename.clone()));
                }
            }
        }

        self.dispatch_file_block_requests();

        Ok(())
    }

    pub fn start_background_tasks(self: &Arc<Self>) {
        let file_sync_manager = self.clone();
        tokio::spawn(async move {
            file_sync_manager.look_for_file_changes().await;
        });

        let file_sync_manager = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

            loop {
                interval.tick().await;
                file_sync_manager.dispatch_file_block_requests();

                if let Err(err) = file_sync_manager.delete_log.lock().await.save().await {
                    println!("{:?}", err);
                }
            }
        });
    }

    fn request_sync(&self, commands_sender: &mut SyncCommandsSender) -> tokio::io::Result<()> {
        let sync_id = self.next_sync_request_id.load(Ordering::SeqCst);
        println!("Sending sync request #{}", sync_id);
        commands_sender.send(SyncCommand::RequestFileSync(sync_id))
            .map_err(|_| tokio::io::Error::from(ErrorKind::Other))
    }

    fn next_sync(&self) {
        self.next_sync_request_id.fetch_add(1, Ordering::SeqCst);
    }

    fn dispatch_file_block_requests(&self) {
        self.file_block_request_dispatcher.dispatch(|channel_id, filename, _| {
            self.failed_file_sync(channel_id, filename.to_owned());
        });
    }

    async fn look_for_file_changes(&self) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        let last_files = files::list_files_as_map(&self.folder).await;
        self.file_changes_finder.lock().unwrap().set_last_files(last_files);

        loop {
            interval.tick().await;

            let new_files = files::list_files_as_map(&self.folder).await;

            let file_changes = self.file_changes_finder.lock().unwrap().find_changes(new_files);
            for modified_file in file_changes.modified {
                let filename = modified_file.path.to_str().unwrap().to_owned();
                println!("Changed file '{}' at {}.", filename, modified_file.modified);
                self.start_file_sync_all(filename, true).await;
            }

            let mut delete_log_guard = self.delete_log.lock().await;
            for deleted_file in file_changes.deleted {
                let filename = deleted_file.path.to_str().unwrap().to_owned();
                println!("Deleted file '{}'", filename);
                delete_log_guard.add_entry(&filename, deleted_file.modified);
                self.clients_manager.send_commands_all(SyncCommand::DeleteFile(filename, deleted_file.modified));
            }
        }
    }

    fn received_file_block(&self, channel_id: ChannelId, filename: &str, block: &FileBlock, content_size: usize) {
        let mut file_sync_status_guard = self.files_sync_status.lock().unwrap();
        if let Some(file_sync_status) = file_sync_status_guard.get_mut(filename) {
            let added = file_sync_status.done_blocks.insert(block.number);

            if added {
                self.file_block_request_dispatcher.received_block(channel_id, filename, block);

                println!(
                    "Received file block: {}/{} ({} bytes), got {}/{}",
                    filename,
                    block.number,
                    content_size,
                    file_sync_status.done_blocks.len(),
                    file_sync_status.request.num_blocks
                );
            } else {
                println!(
                    "Received file block: {}/{} again",
                    filename,
                    block.number
                );
            }
        }
    }

    async fn try_complete_file_sync(&self, filename: &str, modified: ModifiedTime) -> tokio::io::Result<bool> {
        if self.files_sync_status.lock().unwrap().is_done(filename) {
            let tmp_write_path = self.files_sync_status.lock().unwrap().get_tmp_write_path(filename).unwrap().0.clone();
            self.file_changes_finder.lock().unwrap().add_external(PathBuf::from(filename), modified);

            files::make_active(&tmp_write_path, &self.folder.join(filename)).await?;

            let redistribute = self.files_sync_status.lock().unwrap().remove_success(filename);
            println!("Completed sync of file: {}", filename);
            return Ok(redistribute);
        }

        Ok(false)
    }

    async fn start_file_sync_all(&self, filename: String, subset: bool) {
        if let Ok(command) = files::start_file_sync(&self.folder, filename, true).await {
            if subset {
                self.clients_manager.send_commands_random_subset(command);
            } else {
                self.clients_manager.send_commands_all(command);
            }
        }
    }

    fn get_file_from_random(&self,
                            filename: String,
                            modified: ModifiedTime,
                            redistribute: bool,
                            exclude_channels: &HashSet<ChannelId>) -> tokio::io::Result<()> {
        self.clients_manager.send_command_random(
            SyncCommand::GetFile(filename, modified, redistribute),
            exclude_channels
        )
    }

    fn failed_file_sync(&self, channel_id: ChannelId, filename: String) {
        if let Some(file_sync_status) = self.files_sync_status.lock().unwrap().remove_failed(&filename) {
            println!("Failed to sync file '{}'.", filename);

            #[allow(unused_must_use)] {
                self.get_file_from_random(
                    filename,
                    file_sync_status.request.modified,
                    file_sync_status.redistribute,
                    &hashset![channel_id]
                );
            }
        }
    }

    pub fn next_commands_channel_id(&self) -> ChannelId {
        self.clients_manager.next_commands_channel_id()
    }

    pub fn add_client(&self, client_id: ClientId, channel_id: ChannelId, commands_sender: SyncCommandsSender) {
        self.clients_manager.add_client(client_id, channel_id, commands_sender);
    }

    pub fn remove_client(&self, client_id: ClientId) {
        self.clients_manager.remove_client(client_id);
    }

    pub fn remove_active_requests(&self, channel_id: ChannelId) {
        if let Some(requests) = self.file_block_request_dispatcher.remove_active_requests(channel_id) {
            for request in requests {
                self.failed_file_sync(channel_id, request.0);
            }
        }
    }
}