use std::path::{Path, PathBuf};

use crypto::sha2::Sha256;
use crypto::digest::Digest;

use tokio::io::{AsyncSeekExt, SeekFrom, AsyncWriteExt, ErrorKind, AsyncReadExt};

use serde::{Deserialize, Serialize};

use crate::filesync::{SyncCommand};

#[derive(Clone, Debug)]
pub struct File {
    pub path: PathBuf,
    pub modified: f64
}

impl File {
    pub fn new(path: PathBuf, modified: f64) -> File {
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
                let modified = modified.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64();

                files.push(File::new(path, modified));
            }
        }
    }

    Ok(files)
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
    pub modified: f64,
    pub block_size: u64,
    pub num_blocks: u64
}

impl FileRequest {
    pub fn new(size: u64, modified: f64, block_size: u64) -> FileRequest {
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
        file_metadata.modified().unwrap().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64(),
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
                              modified: f64,
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

    utime::set_file_times(
        &path,
        modified as i64,
        modified as i64
    ).map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;

    Ok(())
}

pub fn hash_file_block(offset: u64, content: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.input(&offset.to_le_bytes());
    hasher.input(content);
    hasher.result_str()
}