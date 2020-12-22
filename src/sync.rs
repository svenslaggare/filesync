use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::iter::FromIterator;

use crate::files::File;

pub enum SyncAction {
    GetFile(String),
    SendFile(String),
    DeleteFile(String)
}

pub fn compute_sync_actions(files: Vec<File>, other_files: Vec<File>, two_way: bool) -> Vec<SyncAction> {
    let files_map = HashMap::<PathBuf, &File>::from_iter(files.iter().map(|file| (file.path.clone(), file)));
    let other_files_map = HashMap::<PathBuf, &File>::from_iter(other_files.iter().map(|file| (file.path.clone(), file)));

    let mut actions = Vec::new();
    let send_or_get_actions = |actions: &mut Vec<SyncAction>,
                               files1: &Vec<File>,
                               files2: &HashMap<PathBuf, &File>,
                               send: bool| {
        for file1 in files1 {
            let sync_file = if let Some(file2) = files2.get(&file1.path) {
                file1.modified as u64 > file2.modified as u64
            } else {
                true
            };

            if sync_file {
                let filename = file1.path.to_str().unwrap().to_owned();

                if send {
                    actions.push(SyncAction::SendFile(filename));
                } else {
                    actions.push(SyncAction::GetFile(filename));
                }
            }
        }
    };

    let delete_actions = |actions: &mut Vec<SyncAction>,
                          files1: &Vec<File>,
                          files2: &HashMap<PathBuf, &File>| {
        for file1 in files1 {
            if !files2.contains_key(&file1.path) {
                let filename = file1.path.to_str().unwrap().to_owned();
                actions.push(SyncAction::DeleteFile(filename));
            }
        }
    };

    send_or_get_actions(&mut actions, &files, &other_files_map, true);

    if two_way {
        send_or_get_actions(&mut actions, &other_files, &files_map, false);
    } else {
        delete_actions(&mut actions, &other_files, &files_map);
    }

    actions
}

pub struct FileChanges {
    pub modified: Vec<File>,
    pub deleted: Vec<File>
}

pub struct FileChangesFinder {
    external_modifies: HashSet<(PathBuf, u64)>,
    external_deletes: HashSet<PathBuf>,
    last_files: HashMap<PathBuf, File>
}

impl FileChangesFinder {
    pub fn new() -> FileChangesFinder {
        FileChangesFinder {
            external_modifies: HashSet::new(),
            external_deletes: HashSet::new(),
            last_files: HashMap::new()
        }
    }

    pub fn add_external(&mut self, path: PathBuf, modified: u64) {
        self.external_modifies.insert((path, modified));
    }

    pub fn add_external_delete(&mut self, path: PathBuf) {
        self.external_deletes.insert(path);
    }

    pub fn set_last_files(&mut self, files: HashMap<PathBuf, File>) {
        self.last_files = files;
    }

    pub fn find_changes(&mut self, new_files: HashMap<PathBuf, File>) -> FileChanges {
        let mut modified_files = Vec::new();

        for new_file in new_files.values() {
            let file_change = if let Some(old_file) = self.last_files.get(&new_file.path) {
                if new_file.modified as u64 > old_file.modified as u64 {
                    Some(new_file.clone())
                } else {
                    None
                }
            } else {
                Some(new_file.clone())
            };

            if let Some(file_change) = file_change {
                if !self.external_modifies.remove(&(file_change.path.clone(), file_change.modified as u64)) {
                    modified_files.push(file_change);
                }
            }
        }

        let mut deleted_files = Vec::new();
        for old_file in self.last_files.values() {
            if !new_files.contains_key(&old_file.path) {
                if !self.external_deletes.remove(&old_file.path) {
                    deleted_files.push(old_file.clone());
                }
            }
        }

        self.last_files = new_files;
        FileChanges {
            modified: modified_files,
            deleted: deleted_files
        }
    }
}