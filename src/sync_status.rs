use std::path::{PathBuf, Path};
use std::collections::{HashMap, HashSet};

use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;

use crate::files::{FileRequest, ModifiedTime};

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
}

pub struct FilesSyncStatus {
    files: HashMap<String, FileSyncStatus>
}

impl FilesSyncStatus {
    pub fn new() -> FilesSyncStatus {
        FilesSyncStatus {
            files: HashMap::new()
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
               redistribute: bool) -> FileRequest {
        self.files.insert(
            filename.to_owned(),
            FileSyncStatus::new(folder, request, redistribute)
        );

        self.files[filename].request.clone()
    }

    pub fn remove_success(&mut self, filename: &str) -> bool {
        self.files
            .remove(filename)
            .map(|file_sync_status| file_sync_status.redistribute).unwrap_or(false)
    }

    pub fn remove_failed(&mut self, filename: &str) -> Option<FileSyncStatus> {
        self.files.remove(filename)
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
