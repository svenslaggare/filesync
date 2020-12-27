use std::path::{PathBuf, Path};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Mutex};
use std::sync::atomic::{AtomicI64, Ordering};

use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;

use crate::files::{FileRequest, ModifiedTime, FileBlock};
use crate::filesync::{ChannelId, SyncCommandsSender, SyncCommand};

#[derive(Clone)]
pub struct FileSyncStatus {
    pub tmp_write_path: PathBuf,
    pub request: FileRequest,
    pub done_blocks: HashSet<u64>,
    pub redistribute: bool
}

impl FileSyncStatus {
    pub fn new(folder: &Path, request: FileRequest, redistribute: bool) -> FileSyncStatus {
        let tmp_filename: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .map(char::from)
            .take(20)
            .collect();

        FileSyncStatus {
            tmp_write_path: folder.join(".filesync").join(&tmp_filename),
            request,
            done_blocks: HashSet::new(),
            redistribute
        }
    }

    pub fn is_done(&self) -> bool {
        self.done_blocks.len() as u64 == self.request.num_blocks
    }

    pub fn file_blocks(&self) -> Vec<FileBlock> {
        self.request.file_blocks()
            .filter(|block| !self.done_blocks.contains(&block.number))
            .collect()
    }
}

pub struct FilesSyncStatus {
    files: HashMap<String, FileSyncStatus>,
    partial_files: HashMap<(String, ModifiedTime), FileSyncStatus>
}

impl FilesSyncStatus {
    pub fn new() -> FilesSyncStatus {
        FilesSyncStatus {
            files: HashMap::new(),
            partial_files: HashMap::new()
        }
    }

    pub fn get_tmp_write_path(&self, filename: &str) -> Option<(PathBuf, ModifiedTime)> {
        self.files
            .get(filename)
            .map(|file_sync_status| {
                (file_sync_status.tmp_write_path.clone(), file_sync_status.request.modified)
            })
    }

    pub fn add(&mut self,
               folder: &Path,
               filename: &str,
               request: FileRequest,
               redistribute: bool) -> Vec<FileBlock> {
        if let Some(partial_sync) = self.partial_files.remove(&(filename.to_owned(), request.modified)) {
            self.files.insert(
                filename.to_owned(),
                partial_sync
            );
        } else {
            self.files.insert(
                filename.to_owned(),
                FileSyncStatus::new(folder, request, redistribute)
            );
        }

        self.files[filename].file_blocks()
    }

    pub fn remove_success(&mut self, filename: &str) -> bool {
        self.files
            .remove(filename)
            .map(|file_sync_status| file_sync_status.redistribute).unwrap_or(false)
    }

    pub fn remove_failed(&mut self, filename: &str) -> Option<FileSyncStatus> {
        if let Some(file_sync_status) = self.files.remove(filename) {
            self.partial_files.insert(
                (filename.to_owned(), file_sync_status.request.modified),
                file_sync_status.clone()
            );

            Some(file_sync_status)
        } else {
            None
        }
    }

    pub fn get_mut(&mut self, filename: &str) -> Option<&mut FileSyncStatus> {
        self.files.get_mut(filename)
    }

    pub fn any_active(&self) -> bool {
        !self.files.is_empty()
    }

    pub fn is_syncing(&self, filename: &str) -> bool {
        self.files.contains_key(filename)
    }

    pub fn is_done(&self, filename: &str) -> bool {
        self.files.get(filename).map(|file| file.is_done()).unwrap_or(false)
    }
}

pub struct FileBlockRequest {
    pub commands_sender: SyncCommandsSender,
    pub filename: String,
    pub block: FileBlock
}

pub struct FileBlockRequestDispatcher {
    queue: Mutex<HashMap<ChannelId, VecDeque<FileBlockRequest>>>,
    active_requests: Mutex<HashMap<ChannelId, HashSet<(String, FileBlock)>>>,
    num_active: AtomicI64
}

impl FileBlockRequestDispatcher {
    pub fn new() -> FileBlockRequestDispatcher {
        FileBlockRequestDispatcher {
            queue: Mutex::new(HashMap::new()),
            active_requests: Mutex::new(HashMap::new()),
            num_active: AtomicI64::new(0),
        }
    }

    pub fn dispatch<F: Fn(ChannelId, FileBlockRequest)>(&self, on_failed: F) {
        let mut request_queue_guard = self.queue.lock().unwrap();

        while self.can_dispatch() {
            let mut any_left = false;
            for (channel_id, channel_queue) in request_queue_guard.iter_mut() {
                while let Some(block_request) = channel_queue.pop_front() {
                    any_left = true;
                    let command = SyncCommand::GetFileBlock {
                        filename: block_request.filename.clone(),
                        block: block_request.block.clone()
                    };

                    match block_request.commands_sender.send(command) {
                        Ok(()) => {
                            self.start_sending(*channel_id, block_request);
                            break;
                        }
                        Err(command) => {
                            match command.0 {
                                SyncCommand::GetFileBlock { .. } => {
                                    on_failed(*channel_id, block_request);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }

            if !any_left {
                break;
            }
        }

        request_queue_guard.retain(|_, channel_queue| !channel_queue.is_empty());
    }

    fn start_sending(&self, channel_id: ChannelId, request: FileBlockRequest) {
        self.num_active.fetch_add(1, Ordering::SeqCst);
        self.active_requests
            .lock().unwrap()
            .entry(channel_id)
            .or_insert_with(|| HashSet::new())
            .insert((request.filename, request.block));
    }

    fn can_dispatch(&self) -> bool {
        self.num_active.load(Ordering::SeqCst) < 10
    }

    pub fn enqueue(&self,
                   channel_id: ChannelId,
                   commands_sender: SyncCommandsSender,
                   filename: String,
                   blocks: Vec<FileBlock>) {
        let mut request_queue_guard = self.queue.lock().unwrap();
        let channel_request_queue = request_queue_guard.entry(channel_id).or_insert_with(|| VecDeque::new());

        for block in blocks {
            channel_request_queue.push_back(FileBlockRequest {
                commands_sender: commands_sender.clone(),
                filename: filename.clone(),
                block
            });
        }
    }

    pub fn received_block(&self, channel_id: ChannelId, filename: &str, block: &FileBlock) {
        self.num_active.fetch_sub(1, Ordering::SeqCst);
        if let Some(channel_blocks) = self.active_requests.lock().unwrap().get_mut(&channel_id) {
            channel_blocks.remove(&(filename.to_owned(), block.clone()));
        }
    }

    pub fn remove_active_requests(&self, channel_id: ChannelId) -> Option<HashSet<(String, FileBlock)>> {
        let active_requests = self.active_requests.lock().unwrap().remove(&channel_id);
        if let Some(active_requests) = active_requests.as_ref() {
            self.num_active.fetch_sub(active_requests.len() as i64, Ordering::SeqCst);
        }

        active_requests
    }
}
