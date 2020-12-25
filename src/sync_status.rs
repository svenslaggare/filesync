use std::path::{PathBuf, Path};
use std::collections::{HashMap, HashSet};

use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;

use crate::files::{FileRequest, ModifiedTime, FileBlock};

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
