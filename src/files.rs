use std::path::{Path, PathBuf};

use crypto::sha2::Sha256;
use crypto::digest::Digest;

use tokio::io::{AsyncSeekExt, SeekFrom, AsyncWriteExt, ErrorKind, AsyncReadExt};

use serde::{Deserialize, Serialize};

use crate::filesync::{SyncCommand};

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize, Hash)]
pub struct ModifiedTime {
    seconds: u64,
    microseconds: u64
}

impl ModifiedTime {
    pub fn from_unix_seconds(time: f64) -> ModifiedTime {
        ModifiedTime {
            seconds: time as u64,
            microseconds: ((time - time.floor()) * 1.0E6) as u64
        }
    }

    pub fn from_system_time(time: std::time::SystemTime) -> ModifiedTime {
       let duration = time.duration_since(std::time::UNIX_EPOCH).unwrap();
        ModifiedTime {
            seconds: duration.as_secs() as u64,
            microseconds: duration.subsec_micros() as u64
        }
    }

    pub fn seconds(&self) -> u64 {
        self.seconds
    }

    pub fn microseconds(&self) -> u64 {
        self.microseconds
    }

    pub fn to_unix_seconds(&self) -> f64 {
        self.seconds as f64 + self.microseconds as f64 / 1.0E6
    }

    pub fn is_newer(&self, other: &ModifiedTime) -> bool {
        if self.seconds > other.seconds {
            return true;
        } else if self.seconds == other.seconds {
            return self.microseconds > other.microseconds;
        }

        return false;
    }
}

impl std::fmt::Display for ModifiedTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_unix_seconds())
    }
}

#[derive(Clone, Debug)]
pub struct File {
    pub path: PathBuf,
    pub modified: ModifiedTime
}

impl File {
    pub fn new(path: PathBuf, modified: ModifiedTime) -> File {
        File {
            path,
            modified
        }
    }
}

pub async fn list_files(folder: &Path) -> tokio::io::Result<Vec<File>> {
    let mut files = Vec::new();

    let mut stack = vec![folder.to_path_buf()];
    while let Some(current_folder) = stack.pop() {
        let mut read_dir = tokio::fs::read_dir(current_folder).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            if entry.path().is_dir() && !entry.path().strip_prefix(folder).unwrap().starts_with(".filesync") {
                stack.push(entry.path());
            }

            if entry.path().is_file() {
                let path = entry.path().strip_prefix(folder).unwrap().to_path_buf();

                let metadata = entry.metadata().await?;

                let modified = metadata.modified().unwrap();
                let modified = ModifiedTime::from_system_time(modified);

                files.push(File::new(path, modified));
            }
        }
    }

    Ok(files)
}

pub async fn remote_is_newer(path: &Path, request_time: &ModifiedTime) -> bool {
    tokio::fs::metadata(path).await.map(|metadata| {
        let modified = metadata.modified().unwrap();
        let modified = ModifiedTime::from_system_time(modified);
        request_time.is_newer(&modified)
    }).unwrap_or(true)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileBlock {
    pub number: u64,
    pub size: u64
}

impl FileBlock {
    pub fn offset(&self) -> u64 {
        self.number * self.size
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileRequest {
    pub size: u64,
    pub modified: ModifiedTime,
    pub block_size: u64,
    pub num_blocks: u64
}

impl FileRequest {
    pub fn new(size: u64, modified: ModifiedTime, block_size: u64) -> FileRequest {
        let num_blocks = (size + block_size - 1) / block_size;

        FileRequest {
            size,
            modified,
            block_size,
            num_blocks
        }
    }

    pub fn file_blocks(&self) -> impl Iterator<Item=FileBlock> {
        let block_size = self.block_size;
        (0..self.num_blocks).map(move |number| FileBlock { number, size: block_size })
    }
}

pub async fn start_file_sync(folder: &Path, filename: String) -> tokio::io::Result<SyncCommand> {
    println!("Sending file: {}", filename);

    let path = folder.join(&filename);

    let file_metadata = tokio::fs::metadata(&path).await?;
    let file_request = FileRequest::new(
        file_metadata.len(),
        ModifiedTime::from_system_time(file_metadata.modified().unwrap()),
        32 * 1024
    );

    Ok(
        SyncCommand::StartSyncFile {
            filename: filename.clone(),
            request: file_request
        }
    )
}

pub async fn send_file_block(folder: &Path,
                             filename: String,
                             file_block: &FileBlock) -> tokio::io::Result<SyncCommand> {
    let path = folder.join(&filename);
    let mut file = tokio::fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .await?;

    let offset = file_block.offset();
    let file_size = file.metadata().await?.len();

    file.seek(SeekFrom::Start(offset)).await?;
    let bytes_to_read = file_block.size.min(file_size - offset);

    let mut buffer = vec![0; bytes_to_read as usize];
    file.read_exact(&mut buffer).await?;
    let hash = hash_file_block(offset, &buffer);

    Ok(
        SyncCommand::FileBlock {
            filename: filename.clone(),
            block: file_block.clone(),
            hash,
            content: buffer
        }
    )
}

pub async fn write_file_block(path: &Path,
                              file_block: &FileBlock,
                              modified: ModifiedTime,
                              content: &[u8]) -> tokio::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&path)
        .await?;

    file.seek(SeekFrom::Start(file_block.offset())).await?;
    file.write_all(content).await?;
    file.flush().await?;

    utime(
        &path,
        &modified,
        &modified
    ).map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;

    Ok(())
}

pub fn hash_file_block(offset: u64, content: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.input(&offset.to_le_bytes());
    hasher.input(content);
    hasher.result_str()
}

fn utime<P: AsRef<Path>>(path: P, atime: &ModifiedTime, mtime: &ModifiedTime) -> std::io::Result<()> {
    use libc::{c_char, c_int, time_t, timeval, suseconds_t};
    use std::ffi::CString;
    use std::os::unix::prelude::*;
    extern "C" {
        fn utimes(name: *const c_char, times: *const timeval) -> c_int;
    }

    let path = CString::new(path.as_ref().as_os_str().as_bytes())?;
    let atime = timeval {
        tv_sec: atime.seconds() as time_t,
        tv_usec: atime.microseconds() as suseconds_t,
    };
    let mtime = timeval {
        tv_sec: mtime.seconds() as time_t,
        tv_usec: mtime.microseconds() as suseconds_t,
    };
    let times = [atime, mtime];

    let ret = unsafe { utimes(path.as_ptr(), times.as_ptr()) };
    if ret == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}