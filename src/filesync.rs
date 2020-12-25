use std::path::{PathBuf, Path};
use std::collections::{HashMap, VecDeque, HashSet};
use std::net::SocketAddr;
use std::sync::{Mutex, Arc};
use std::iter::FromIterator;
use std::sync::atomic::{AtomicI64, Ordering, AtomicU64};

use rand::{thread_rng};
use rand::seq::SliceRandom;

use tokio::io::{AsyncReadExt, AsyncWriteExt, ErrorKind};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use serde::{Deserialize, Serialize};

use crate::files::{FileBlock, File, FileRequest, hash_file_block, ModifiedTime};
use crate::{files, sync};
use crate::sync::{SyncAction, FileChangesFinder};
use crate::tracker::ClientId;
use crate::sync_status::FilesSyncStatus;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SyncCommand {
    RequestFileSync(u64, bool),
    AcceptedFileSync(u64, bool),
    SyncFiles { files: Vec<(String, ModifiedTime)>, two_way: bool },
    GetFile(String, ModifiedTime),
    GetFileDenied(String, ModifiedTime),
    SendFile(String, ModifiedTime),
    StartSyncFile { filename: String, request: FileRequest, redistribute: bool },
    GetFileBlock { filename: String, block: FileBlock },
    FileBlock { filename: String, block: FileBlock, hash: String, content: Vec<u8> },
    DeleteFile(String)
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

pub type SyncCommandsSender = mpsc::UnboundedSender<SyncCommand>;
pub type SyncCommandsReceiver = mpsc::UnboundedReceiver<SyncCommand>;

pub struct FileSyncManager {
    pub folder: PathBuf,
    files_sync_status: Mutex<FilesSyncStatus>,
    clients_commands_sender: Mutex<HashMap<ClientId, SyncCommandsSender>>,
    file_changes_finder: Mutex<FileChangesFinder>,
    request_file_block_queue: Mutex<VecDeque<(SyncCommandsSender, SyncCommand)>>,
    num_active_file_block_requests: AtomicI64,
    next_sync_request_id: AtomicU64,
    sync_requests: Mutex<HashSet<u64>>
}

impl FileSyncManager {
    pub fn new(folder: PathBuf) -> FileSyncManager {
        FileSyncManager {
            folder,
            files_sync_status: Mutex::new(FilesSyncStatus::new()),
            clients_commands_sender: Mutex::new(HashMap::new()),
            file_changes_finder: Mutex::new(FileChangesFinder::new()),
            request_file_block_queue: Mutex::new(VecDeque::new()),
            num_active_file_block_requests: AtomicI64::new(0),
            next_sync_request_id: AtomicU64::new(1),
            sync_requests: Mutex::new(HashSet::new())
        }
    }

    pub async fn run(&self,
                     client: TcpStream,
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
            self.request_sync(false, &mut commands_sender)?;
        }

        loop {
            let command = SyncCommand::receive_command(&mut client_reader).await?;
            self.handle(
                destination_address,
                &mut commands_sender,
                command
            ).await?;
        }
    }

    pub async fn handle(&self,
                        destination_address: SocketAddr,
                        commands_sender: &mut SyncCommandsSender,
                        command: SyncCommand) -> tokio::io::Result<()> {
        match command {
            SyncCommand::RequestFileSync(id, two_way) => {
                if !self.files_sync_status.lock().unwrap().any_active() {
                    commands_sender.send(SyncCommand::AcceptedFileSync(id, two_way))
                        .map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
                } else {
                    println!("Denied sync request #{}", id);
                }
            }
            SyncCommand::AcceptedFileSync(id, two_way) => {
                println!("Got sync request id #{}", id);

                let sync = self.sync_requests.lock().unwrap().insert(id);
                if sync {
                    println!("Requesting sync of files with {}.", destination_address);

                    let files = files::list_files(&self.folder).await?;

                    commands_sender.send(SyncCommand::SyncFiles {
                        files: files
                            .iter()
                            .map(|file| (file.path.to_str().unwrap().to_owned(), file.modified))
                            .collect::<Vec<_>>(),
                        two_way
                    }).map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;

                    self.next_sync();
                }
            }
            SyncCommand::SyncFiles { files: other_files, two_way } => {
                println!("Syncing files...");

                let files = files::list_files(&self.folder).await?;
                let other_files = other_files
                    .into_iter()
                    .map(|file| File::new(PathBuf::from(file.0), file.1))
                    .collect::<Vec<_>>();

                for action in sync::compute_sync_actions(files, other_files, two_way) {
                    match action {
                        SyncAction::GetFile(filename, modified) => {
                            println!("Get file: {}", filename);
                            commands_sender.send(SyncCommand::GetFile(filename, modified))
                                .map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
                        }
                        SyncAction::SendFile(filename, modified) => {
                            println!("Sending file: {}", filename);
                            commands_sender.send(SyncCommand::SendFile(filename, modified))
                                .map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
                        }
                        SyncAction::DeleteFile(filename) => {
                            println!("Delete file: {}", filename);
                            commands_sender.send(SyncCommand::DeleteFile(filename))
                                .map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
                        }
                    }
                }
            }
            SyncCommand::GetFile(filename, modified) => {
                if files::has_file(&self.folder.join(&filename), &modified).await {
                    commands_sender.send(files::start_file_sync(
                        &self.folder,
                        filename,
                        false
                    ).await?).map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
                } else {
                    println!("Don't got file {} @ {}", filename, modified.to_unix_seconds());
                    commands_sender.send(SyncCommand::GetFileDenied(filename, modified))
                        .map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
                }
            }
            SyncCommand::GetFileDenied(filename, modified) => {
                self.get_file_from_random(filename, modified)?;
            },
            SyncCommand::SendFile(filename, modified) => {
                self.get_file_from_random(filename, modified)?;
            },
            SyncCommand::StartSyncFile { filename, request, redistribute } => {
                if files::is_remote_newer(&self.folder.join(&filename), &request.modified).await {
                    if !self.files_sync_status.lock().unwrap().is_syncing(&filename) {
                        let mut request_file_block_queue_guard = self.request_file_block_queue.lock().unwrap();

                        println!(
                            "Starting sync of file: {} ({} bytes), redistribute: {}",
                            filename,
                            request.size,
                            redistribute
                        );

                        for block in self.start_file_sync(&filename, request, redistribute).file_blocks() {
                            request_file_block_queue_guard.push_back((
                                commands_sender.clone(),
                                SyncCommand::GetFileBlock {
                                    filename: filename.clone(),
                                    block,
                                }
                            ));
                        }
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

                        self.received_file_block(&filename, &block, content.len());
                        if self.try_complete_file_sync(&filename, modified).await? {
                            self.send_commands_all(SyncCommand::SendFile(filename, modified));
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
            SyncCommand::DeleteFile(filename) => {
                println!("Deleting file: {}", filename);
                self.file_changes_finder.lock().unwrap().add_external_delete(PathBuf::from(filename.clone()));
                tokio::fs::remove_file(self.folder.join(PathBuf::from(filename))).await?;
            }
        }

        self.dispatch_file_block_requests();

        Ok(())
    }

    pub fn start_background_tasks(self: &Arc<FileSyncManager>) {
        let file_sync_manager = self.clone();
        tokio::spawn(async move {
            file_sync_manager.look_for_file_changes().await;
        });

        let file_sync_manager = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));

            loop {
                interval.tick().await;
                file_sync_manager.dispatch_file_block_requests();
            }
        });
    }

    fn request_sync(&self,
                        two_way: bool,
                        commands_sender: &mut SyncCommandsSender) -> tokio::io::Result<()> {
        let sync_id = self.next_sync_request_id.load(Ordering::SeqCst);
        println!("Sending sync request #{}", sync_id);
        commands_sender.send(SyncCommand::RequestFileSync(sync_id, two_way))
            .map_err(|_| tokio::io::Error::from(ErrorKind::Other))
    }

    fn next_sync(&self) {
        self.next_sync_request_id.fetch_add(1, Ordering::SeqCst);
    }

    fn dispatch_file_block_requests(&self) {
        let mut request_file_block_queue_guard = self.request_file_block_queue.lock().unwrap();

        while self.num_active_file_block_requests.load(Ordering::SeqCst) < 10 {
            if let Some((commands_sender, command)) = request_file_block_queue_guard.pop_front() {
                let command_clone = command.clone();
                if commands_sender.send(command).is_ok() {
                    self.num_active_file_block_requests.fetch_add(1, Ordering::SeqCst);
                } else {
                    match command_clone {
                        SyncCommand::GetFileBlock { filename, .. } => {
                            if let Some(file_sync_status) = self.files_sync_status.lock().unwrap().remove_failed(&filename) {
                                println!("Failed to sync file '{}'.", filename);

                                #[allow(unused_must_use)] {
                                    self.get_file_from_random(filename, file_sync_status.request.modified);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            } else {
                break;
            }
        }
    }

    async fn look_for_file_changes(&self) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        let last_files = list_files_as_map(&self.folder).await;
        self.file_changes_finder.lock().unwrap().set_last_files(last_files);

        loop {
            interval.tick().await;

            let new_files = list_files_as_map(&self.folder).await;

            let file_changes = self.file_changes_finder.lock().unwrap().find_changes(new_files);
            for modified_file in file_changes.modified {
                let filename = modified_file.path.to_str().unwrap().to_owned();
                println!("Changed file '{}' at {}.", filename, modified_file.modified);
                self.start_file_sync_all(filename, true).await;
            }

            for deleted_file in file_changes.deleted {
                let filename = deleted_file.path.to_str().unwrap().to_owned();
                println!("Deleted file '{}'", filename);
                self.send_commands_all(SyncCommand::DeleteFile(filename));
            }
        }
    }

    pub fn start_file_sync(&self, filename: &str, request: FileRequest, redistribute: bool) -> FileRequest {
        self.files_sync_status.lock().unwrap().add(&self.folder, filename, request, redistribute)
    }

    fn received_file_block(&self, filename: &str, block: &FileBlock, content_size: usize) {
        let mut file_sync_status_guard = self.files_sync_status.lock().unwrap();
        if let Some(file_sync_status) = file_sync_status_guard.get_mut(filename) {
            file_sync_status.done_blocks.insert(block.number);

            self.num_active_file_block_requests.fetch_sub(1, Ordering::SeqCst);

            println!(
                "Received file block: {}/{} ({} bytes), got {}/{}",
                filename,
                block.number,
                content_size,
                file_sync_status.done_blocks.len(),
                file_sync_status.request.num_blocks
            );
        }
    }

    async fn try_complete_file_sync(&self, filename: &str, modified: ModifiedTime) -> tokio::io::Result<bool> {
        if self.files_sync_status.lock().unwrap().is_done(filename) {
            let tmp_write_path = self.files_sync_status.lock().unwrap().get_tmp_write_path(filename).unwrap().0.clone();
            self.file_changes_finder.lock().unwrap().add_external(PathBuf::from(filename), modified);

            let path = self.folder.join(filename);
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }

            tokio::fs::rename(tmp_write_path, path).await?;

            let redistribute = self.files_sync_status.lock().unwrap().remove_success(filename);
            println!("Completed sync of file: {}", filename);
            return Ok(redistribute);
        }

        Ok(false)
    }

    async fn start_file_sync_all(&self, filename: String, subset: bool) {
        if let Ok(command) = files::start_file_sync(&self.folder, filename, true).await {
            if subset {
                self.send_commands_random_subset(command);
            } else {
                self.send_commands_all(command);
            }
        }
    }

    fn get_file_from_random(&self, filename: String, modified: ModifiedTime) -> tokio::io::Result<()> {
        let command = SyncCommand::GetFile(filename, modified);
        for _ in 0..10 {
            if self.send_command_random(command.clone()) {
                return Ok(());
            }
        }

        return Err(tokio::io::Error::from(ErrorKind::Other));
    }

    pub fn add_client(&self, client_id: ClientId, commands_sender: SyncCommandsSender) {
        self.clients_commands_sender.lock().unwrap().insert(client_id, commands_sender);
    }

    pub fn remove_client(&self, client_id: ClientId) {
        self.clients_commands_sender.lock().unwrap().remove(&client_id);
    }

    pub fn send_commands_all(&self, command: SyncCommand) {
        for commands_sender in self.clients_commands_sender.lock().unwrap().values() {
            if let Err(err) = commands_sender.send(command.clone()) {
                println!("{:?}", err);
            }
        }
    }

    pub fn send_commands_random_subset(&self, command: SyncCommand) -> usize {
        let clients_commands_sender_guard = self.clients_commands_sender.lock().unwrap();

        if !clients_commands_sender_guard.is_empty() {
            let mut keys = clients_commands_sender_guard.keys().collect::<Vec<_>>();
            keys.shuffle(&mut thread_rng());
            // let count = thread_rng().gen_range(1..(keys.len() + 1));
            let count = 1;

            for client in &keys[..count] {
                if let Err(err) = clients_commands_sender_guard[*client].send(command.clone()) {
                    println!("{:?}", err);
                }
            }

            count
        } else {
            0
        }
    }

    pub fn send_command_random(&self, command: SyncCommand) -> bool {
        let clients_commands_sender_guard = self.clients_commands_sender.lock().unwrap();

        if !clients_commands_sender_guard.is_empty() {
            let mut keys = clients_commands_sender_guard.keys().collect::<Vec<_>>();
            keys.shuffle(&mut thread_rng());

            let key = keys[0];
            clients_commands_sender_guard[key].send(command.clone()).is_ok()
        } else {
            false
        }
    }
}

async fn list_files_as_map(folder: &Path) -> HashMap<PathBuf, File> {
    create_files_map(files::list_files(&folder).await.unwrap_or_else(|_| Vec::new()))
}

fn create_files_map(files: Vec<File>) -> HashMap<PathBuf, File> {
    HashMap::from_iter(files.into_iter().map(|file| (file.path.clone(), file)))
}