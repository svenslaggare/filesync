use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::iter::FromIterator;

use serde::{Deserialize, Serialize};

use crate::files::{File, ModifiedTime};

pub enum SyncAction {
    GetFile(String, ModifiedTime),
    SendFile(String, ModifiedTime),
    DeleteFile(String, ModifiedTime)
}

pub fn compute_sync_actions(files: Vec<File>,
                            other_files: Vec<File>,
                            delete_log: &DeleteLog) -> Vec<SyncAction> {
    let files_map = HashMap::<PathBuf, &File>::from_iter(files.iter().map(|file| (file.path.clone(), file)));
    let other_files_map = HashMap::<PathBuf, &File>::from_iter(other_files.iter().map(|file| (file.path.clone(), file)));

    let mut actions = Vec::new();

    for file1 in &files {
        let sync_file = if let Some(file2) = other_files_map.get(&file1.path) {
            file1.modified.is_newer(&file2.modified)
        } else {
            true
        };

        if sync_file {
            let filename = file1.path.to_str().unwrap().to_owned();
            actions.push(SyncAction::SendFile(filename, file1.modified));
        }
    }

    for file1 in &other_files {
        let filename = file1.path.to_str().unwrap().to_owned();
        if let Some(file2) = files_map.get(&file1.path) {
            if file1.modified.is_newer(&file2.modified) {
                actions.push(SyncAction::GetFile(filename, file1.modified));
            }
        } else if delete_log.files.contains_key(&(filename.clone(), file1.modified)) {
            actions.push(SyncAction::DeleteFile(filename, file1.modified));
        } else {
            actions.push(SyncAction::GetFile(filename, file1.modified));
        }
    }

    actions
}

pub struct FileChanges {
    pub modified: Vec<File>,
    pub deleted: Vec<File>
}

pub struct FileChangesFinder {
    external_modifies: HashSet<(PathBuf, ModifiedTime)>,
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

    pub fn add_external(&mut self, path: PathBuf, modified: ModifiedTime) {
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
                if new_file.modified.is_newer(&old_file.modified) {
                    Some(new_file.clone())
                } else {
                    None
                }
            } else {
                Some(new_file.clone())
            };

            if let Some(file_change) = file_change {
                if !self.external_modifies.remove(&(file_change.path.clone(), file_change.modified)) {
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

#[derive(Deserialize, Serialize)]
struct DeleteLogEntry {
    filename: String,
    modified: ModifiedTime,
    created: std::time::SystemTime
}

pub struct DeleteLog {
    folder: PathBuf,
    files: HashMap<(String, ModifiedTime), std::time::SystemTime>
}

impl DeleteLog {
    pub fn new(folder: PathBuf) -> DeleteLog {
        let path = folder.join(".filesync").join("delete_log.json");
        let files = if let Ok(content) = std::fs::read_to_string(&path) {
            serde_json::from_str::<Vec<DeleteLogEntry>>(&content)
                .map(|files| {
                    HashMap::from_iter(files.into_iter().map(|entry| ((entry.filename, entry.modified), entry.created)))
                })
                .unwrap_or_else(|_| HashMap::new())
        } else {
            HashMap::new()
        };

        let mut delete_log = DeleteLog {
            folder,
            files
        };

        delete_log.clean_old_entries();

        delete_log
    }

    pub fn add_entry(&mut self, filename: &str, modified_time: ModifiedTime) {
        self.files.insert(
            (filename.to_owned(), modified_time),
            std::time::SystemTime::now()
        );

        self.clean_old_entries();
    }

    pub async fn save(&self) -> tokio::io::Result<()> {
        let path = self.folder.join(".filesync").join("delete_log.json");
        if let Some(parent) = path.parent() {
            if !path.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }

        tokio::fs::write(
            path,
            serde_json::to_string(
                &self.files
                    .iter()
                    .map(|(key, value)| DeleteLogEntry {
                        filename: key.0.clone(),
                        modified: key.1,
                        created: *value
                    })
                    .collect::<Vec<_>>()
            ).unwrap().into_bytes()
        ).await?;

        Ok(())
    }

    fn clean_old_entries(&mut self) {
        let time_now = std::time::SystemTime::now();
        self.files.retain(|_, value| {
            time_now.duration_since(*value).unwrap().as_secs_f64() <= 60.0
        });
    }
}