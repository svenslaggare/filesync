use std::path::{PathBuf, Path};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Mutex};
use std::sync::atomic::{AtomicI64, Ordering};

use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use rand::seq::SliceRandom;

use serde::{Serialize, Deserialize};

use crate::files::{FileRequest, ModifiedTime, FileBlock};
use crate::filesync::{ChannelId, SyncCommandsSender, SyncCommand};
use crate::tracker::ClientId;
use crate::sync_clients::PeersManager;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileSyncRequest {
    pub filename: String,
    pub modified: ModifiedTime,
    pub redistribute: bool
}

struct FilePeers {
    request: FileSyncRequest,
    created: std::time::Instant,
    last_poll: Option<std::time::Instant>,
    peers: Vec<(ClientId, i32)>
}

impl FilePeers {
    pub fn new(request: FileSyncRequest) -> FilePeers {
        FilePeers {
            request,
            created: std::time::Instant::now(),
            last_poll: None,
            peers: Vec::new()
        }
    }

    pub fn alive_seconds(&self) -> f64 {
        (std::time::Instant::now() - self.created).as_secs_f64()
    }

    pub fn poll_seconds(&self) -> Option<f64> {
        self.last_poll.map(|last_poll| (std::time::Instant::now() - last_poll).as_secs_f64())
    }
}

pub struct FilePeersDiscovery {
    files: HashMap<String, FilePeers>
}

impl FilePeersDiscovery {
    pub fn new() -> FilePeersDiscovery {
        FilePeersDiscovery {
            files: HashMap::new()
        }
    }

    pub fn add_file(&mut self, request: FileSyncRequest) {
        self.files.insert(
            request.filename.clone(),
            FilePeers::new(request)
        );
    }

    pub fn send_discover(&mut self, filename: &str, clients: &PeersManager) {
        if let Some(file_peers) = self.files.get_mut(filename) {
            FilePeersDiscovery::send_discover_internal(filename, file_peers, clients);
        }
    }

    fn send_discover_internal(filename: &str, file_peers: &mut FilePeers, clients: &PeersManager) {
        file_peers.peers.clear();
        file_peers.last_poll = Some(std::time::Instant::now());
        clients.send_commands_all(SyncCommand::GotFileRequest(filename.to_owned(), file_peers.request.modified));
    }

    pub fn add_file_peer(&mut self, filename: &str, client_id: ClientId, availability: i32) {
        if let Some(file_peer) = self.files.get_mut(filename) {
            file_peer.peers.push((client_id, availability));
        }
    }

    pub fn try_select_file_peer(&mut self, filename: &str) -> Option<(ClientId, FileSyncRequest)> {
        if let Some(file_peers) = self.files.get(filename) {
            if !file_peers.peers.is_empty() {
                let mut peers = file_peers.peers.clone();
                let mut rng = thread_rng();

                peers.shuffle(&mut rng);

                for peer in &mut peers {
                    if peer.1 != 0 {
                        peer.1 += rng.gen_range(0..3);
                    }
                }

                let file_peers = self.files.remove(filename).unwrap();

                peers.sort_by_key(|peer| -peer.1);
                peers.first().map(|c| (c.0, file_peers.request))
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn update(&mut self, peers: &PeersManager) {
        let filenames = self.files.keys().cloned().collect::<Vec<_>>();
        for filename in filenames {
            if let Some((client_id, request)) = self.try_select_file_peer(&filename) {
                let sent = peers.send_commands_to(
                    client_id,
                    SyncCommand::GetFile(request.clone())
                ).is_ok();

                if !sent {
                    self.add_file(request);
                }
            }
        }

        self.files.retain(|_, file_peers| !(file_peers.peers.is_empty() && file_peers.alive_seconds() >= 60.0));

        for (filename, file_peers) in self.files.iter_mut() {
            if file_peers.poll_seconds().map(|poll_time| poll_time >= 1.0).unwrap_or(true) {
                FilePeersDiscovery::send_discover_internal(filename, file_peers, peers);
            }
        }
    }
}

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

    pub fn get(&mut self, filename: &str) -> Option<&FileSyncStatus> {
        self.files.get(filename)
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

    pub fn is_newer(&self, filename: &str, modified: ModifiedTime) -> bool {
        self.files.get(filename)
            .map(|file_sync_status| modified.is_newer(&file_sync_status.request.modified))
            .unwrap_or(false)
    }

    pub fn is_done(&self, filename: &str) -> bool {
        self.files.get(filename).map(|file| file.is_done()).unwrap_or(false)
    }
}

struct FileBlocksRequest {
    pub commands_sender: SyncCommandsSender,
    pub filename: String,
    pub modified: ModifiedTime,
    pub blocks: VecDeque<FileBlock>
}

pub struct FileBlockRequestDispatcher {
    queue: Mutex<HashMap<ChannelId, VecDeque<FileBlocksRequest>>>,
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

    pub fn dispatch<F: Fn(ChannelId, &str, FileBlock)>(&self, on_failed: F) {
        let mut request_queue_guard = self.queue.lock().unwrap();

        while self.can_dispatch() {
            let mut any_left = false;
            for (channel_id, channel_queue) in request_queue_guard.iter_mut() {
                if let Some(file_request) = channel_queue.front_mut() {
                    while let Some(block) = file_request.blocks.pop_front() {
                        any_left = true;
                        let command = SyncCommand::GetFileBlock {
                            filename: file_request.filename.clone(),
                            modified: file_request.modified,
                            block: block.clone()
                        };

                        match file_request.commands_sender.send(command) {
                            Ok(()) => {
                                self.start_sending(*channel_id, file_request.filename.clone(), block);
                                break;
                            }
                            Err(command) => {
                                match command.0 {
                                    SyncCommand::GetFileBlock { .. } => {
                                        on_failed(*channel_id, &file_request.filename, block);
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    if file_request.blocks.is_empty() {
                        channel_queue.pop_front();
                    }
                }
            }

            if !any_left {
                break;
            }
        }

        request_queue_guard.retain(|_, channel_queue| !channel_queue.is_empty());
    }

    fn start_sending(&self, channel_id: ChannelId, filename: String, block: FileBlock) {
        self.num_active.fetch_add(1, Ordering::SeqCst);
        self.active_requests
            .lock().unwrap()
            .entry(channel_id)
            .or_insert_with(|| HashSet::new())
            .insert((filename, block));
    }

    fn can_dispatch(&self) -> bool {
        self.num_active.load(Ordering::SeqCst) < 10
    }

    pub fn enqueue(&self,
                   channel_id: ChannelId,
                   commands_sender: SyncCommandsSender,
                   filename: String,
                   modified: ModifiedTime,
                   blocks: Vec<FileBlock>) {
        let mut request_queue_guard = self.queue.lock().unwrap();
        let channel_request_queue = request_queue_guard.entry(channel_id).or_insert_with(|| VecDeque::new());
        channel_request_queue.push_back(FileBlocksRequest {
            commands_sender,
            filename,
            modified,
            blocks: VecDeque::from(blocks)
        });
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

    pub fn remove_file(&self, filename: &str) {
        let mut request_queue_guard = self.queue.lock().unwrap();
        for channel_queue in request_queue_guard.values_mut() {
            channel_queue.retain(|file| file.filename != filename);
        }
    }
}
