use std::path::{PathBuf, Path};
use std::collections::{HashMap, VecDeque, HashSet};
use std::net::SocketAddr;
use std::sync::{Mutex, Arc};
use std::iter::FromIterator;
use std::sync::atomic::{AtomicI64, Ordering};

use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;

use tokio::io::{AsyncReadExt, AsyncWriteExt, ErrorKind};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use serde::{Deserialize, Serialize};

use crate::files::{FileBlock, File, FileRequest, hash_file_block};
use crate::{files, sync};
use crate::sync::{SyncAction, FileChangesFinder};
use crate::p2p::ClientId;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SyncCommand {
    SyncFiles { files: Vec<(String, f64)>, two_way: bool },
    GetFile(String),
    StartSyncFile { filename: String, request: FileRequest },
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

struct FileSyncStatus {
    tmp_write_path: PathBuf,
    request: FileRequest,
    done_blocks: HashSet<u64>
}

impl FileSyncStatus {
    pub fn new(folder: &Path, request: FileRequest) -> FileSyncStatus {
        let tmp_filename: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .map(char::from)
            .take(20)
            .collect();

        FileSyncStatus {
            tmp_write_path: folder.join(".filesync").join(&tmp_filename),
            request,
            done_blocks: HashSet::new()
        }
    }

    pub fn is_done(&self) -> bool {
        self.done_blocks.len() as u64 == self.request.num_blocks
    }
}

pub struct FileSyncManager {
    pub folder: PathBuf,
    files_sync_status: Mutex<HashMap<String, FileSyncStatus>>,
    clients_commands_sender: Mutex<HashMap<ClientId, mpsc::UnboundedSender<SyncCommand>>>,
    file_changes_finder: Mutex<FileChangesFinder>,
    request_file_block_queue: Mutex<VecDeque<(mpsc::UnboundedSender<SyncCommand>, SyncCommand)>>,
    num_active_file_block_requests: AtomicI64
}

impl FileSyncManager {
    pub fn new(folder: PathBuf) -> FileSyncManager {
        FileSyncManager {
            folder,
            files_sync_status: Mutex::new(HashMap::new()),
            clients_commands_sender: Mutex::new(HashMap::new()),
            file_changes_finder: Mutex::new(FileChangesFinder::new()),
            request_file_block_queue: Mutex::new(VecDeque::new()),
            num_active_file_block_requests: AtomicI64::new(0)
        }
    }

    pub async fn handle(&self,
                        commands_sender: &mut mpsc::UnboundedSender<SyncCommand>,
                        command: SyncCommand) -> tokio::io::Result<()> {
        match command {
            SyncCommand::SyncFiles { files: other_files, two_way } => {
                println!("Syncing files...");

                let files = files::list_files(&self.folder).await?;
                let other_files = other_files
                    .into_iter()
                    .map(|file| File::new(PathBuf::from(file.0), file.1))
                    .collect::<Vec<_>>();

                for action in sync::compute_sync_actions(files, other_files, two_way) {
                    match action {
                        SyncAction::GetFile(filename) => {
                            println!("Get file: {}", filename);
                            commands_sender.send(SyncCommand::GetFile(filename))
                                .map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
                        }
                        SyncAction::SendFile(filename) => {
                            commands_sender.send(files::start_file_sync(&self.folder, filename).await?)
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
            SyncCommand::GetFile(filename) => {
                commands_sender.send(files::start_file_sync(
                    &self.folder,
                    filename
                ).await?).map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
            }
            SyncCommand::StartSyncFile { filename, request } => {
                println!("Starting sync of file: {} ({} bytes)", filename, request.size);

                let mut request_file_block_queue_guard = self.request_file_block_queue.lock().unwrap();
                for block in self.start_sync(&filename, request).file_blocks() {
                    request_file_block_queue_guard.push_back((
                        commands_sender.clone(),
                        SyncCommand::GetFileBlock {
                            filename: filename.clone(),
                            block,
                        }
                    ));
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
                    let result = self.files_sync_status.lock().unwrap()
                        .get(&filename)
                        .map(|file_sync_status| {
                            (file_sync_status.tmp_write_path.clone(), file_sync_status.request.modified)
                        });

                    if let Some((tmp_path, modified)) = result {
                        files::write_file_block(
                            &tmp_path,
                            &block,
                            modified,
                            &content,
                        ).await?;

                        self.received_file_block(&filename, &block, content.len());
                        self.try_complete_sync(&filename, modified).await?;
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

    fn dispatch_file_block_requests(&self) {
        let mut request_file_block_queue_guard = self.request_file_block_queue.lock().unwrap();

        while self.num_active_file_block_requests.load(Ordering::SeqCst) < 10 {
            if let Some((commands_sender, command)) = request_file_block_queue_guard.pop_front() {
                if commands_sender.send(command).is_ok() {
                    self.num_active_file_block_requests.fetch_add(1, Ordering::SeqCst);
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
                if let Ok(command) = files::start_file_sync(&self.folder, filename).await {
                    self.send_commands_all(command);
                }
            }

            for deleted_file in file_changes.deleted {
                let filename = deleted_file.path.to_str().unwrap().to_owned();
                println!("Deleted file '{}'", filename);
                self.send_commands_all(SyncCommand::DeleteFile(filename));
            }
        }
    }

    pub fn start_sync(&self, filename: &str, request: FileRequest) -> FileRequest {
        let mut files_sync_status_guard = self.files_sync_status.lock().unwrap();
        files_sync_status_guard.insert(
            filename.to_owned(),
            FileSyncStatus::new(&self.folder, request)
        );

        files_sync_status_guard[filename].request.clone()
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

    async fn try_complete_sync(&self, filename: &str, modified: f64) -> tokio::io::Result<()> {
        if self.files_sync_status.lock().unwrap().get(filename).map(|file| file.is_done()).unwrap_or(false) {
            let tmp_write_path = self.files_sync_status.lock().unwrap()[filename].tmp_write_path.clone();
            self.file_changes_finder.lock().unwrap().add_external(PathBuf::from(filename), modified as u64);

            let path = self.folder.join(filename);
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }

            tokio::fs::rename(tmp_write_path, path).await?;

            self.files_sync_status.lock().unwrap().remove(filename);
            println!("Completed sync of file: {}", filename);
        }

        Ok(())
    }

    pub fn add_client(&self, client_id: ClientId, commands_sender: mpsc::UnboundedSender<SyncCommand>) {
        self.clients_commands_sender.lock().unwrap().insert(client_id, commands_sender);
    }

    pub fn remove_client(&self, client_id: ClientId) {
        self.clients_commands_sender.lock().unwrap().remove(&client_id);
    }

    pub fn send_commands_all(&self, command: SyncCommand) {
        for commands_sender in self.clients_commands_sender.lock().unwrap().values_mut() {
            if let Err(err) = commands_sender.send(command.clone()) {
                println!("{:?}", err);
            }
        }
    }
}

pub async fn run_sync_client(command_handler: Arc<FileSyncManager>,
                             client: TcpStream,
                             commands_channel: (mpsc::UnboundedSender<SyncCommand>,
                                                mpsc::UnboundedReceiver<SyncCommand>)) -> tokio::io::Result<()> {
    let (mut commands_sender, mut commands_receiver) = commands_channel;
    let (mut client_reader, mut client_writer) = client.into_split();

    tokio::spawn(async move {
        while let Some(command) = commands_receiver.recv().await {
            if let Err(_) = command.send_command(&mut client_writer).await {
                break;
            }
        }
    });

    loop {
        let command = SyncCommand::receive_command(&mut client_reader).await?;
        command_handler.handle(
            &mut commands_sender,
            command
        ).await?;
    }
}

pub async fn sync_files<T: AsyncWriteExt + Unpin>(destination: &mut T,
                                                  destination_address: SocketAddr,
                                                  folder: &Path,
                                                  two_way: bool) -> tokio::io::Result<()> {
    println!("Requesting sync of files with {}.", destination_address);
    let files = files::list_files(&folder).await?;

    SyncCommand::SyncFiles {
        files: files
            .iter()
            .map(|file| (file.path.to_str().unwrap().to_owned(), file.modified))
            .collect::<Vec<_>>(),
        two_way
    }.send_command(destination).await?;

    Ok(())
}

async fn list_files_as_map(folder: &Path) -> HashMap<PathBuf, File> {
    create_files_map(files::list_files(&folder).await.unwrap_or_else(|_| Vec::new()))
}

fn create_files_map(files: Vec<File>) -> HashMap<PathBuf, File> {
    HashMap::from_iter(files.into_iter().map(|file| (file.path.clone(), file)))
}