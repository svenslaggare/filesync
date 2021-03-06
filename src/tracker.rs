use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::atomic::{Ordering, AtomicU64};
use std::sync::{Arc, Mutex};
use std::path::PathBuf;

use tokio::net::{TcpStream, TcpListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ErrorKind};
use tokio::sync::mpsc;

use serde::{Serialize, Deserialize};

use crate::filesync::{FileSyncManager};

#[derive(Copy, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientId(pub u64);

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Debug for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TrackerClient {
    pub id: ClientId,
    pub address: SocketAddr
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TrackerCommand {
    Connect(SocketAddr),
    ConnectionInfo { id: ClientId, clients: Vec<TrackerClient> },
    ClientConnected(TrackerClient),
    ClientDisconnected(ClientId)
}

impl TrackerCommand {
    pub async fn receive_command<T: AsyncReadExt + Unpin>(socket: &mut T) -> tokio::io::Result<TrackerCommand> {
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

pub struct TrackerManagerClient {
    client: TrackerClient,
    command_sender: mpsc::UnboundedSender<TrackerCommand>
}

pub struct TrackerManager {
    next_client_id: u64,
    clients: HashMap<ClientId, TrackerManagerClient>
}

impl TrackerManager {
    pub fn new() -> TrackerManager {
        TrackerManager {
            next_client_id: 1,
            clients: HashMap::new()
        }
    }

    pub fn new_client(&mut self,
                      address: SocketAddr,
                      command_sender: mpsc::UnboundedSender<TrackerCommand>) -> ClientId {
        let client_id = ClientId(self.next_client_id);
        self.next_client_id += 1;
        println!("{} = #{}", address, client_id);
        self.clients.insert(
            client_id,
            TrackerManagerClient {
                client: TrackerClient {
                    id: client_id,
                    address,
                },
                command_sender
            }
        );

        let new_client = &self.clients[&client_id].client;
        for client in self.clients.values() {
            if client.client.id != new_client.id {
                #[allow(unused_must_use)] {
                    client.command_sender.send(TrackerCommand::ClientConnected(new_client.clone()));
                }
            }
        }

        return client_id
    }

    pub fn remove_client(&mut self, id: ClientId) {
        println!("Client #{} disconnected", id);
        self.clients.remove(&id);

        for client in self.clients.values() {
            #[allow(unused_must_use)] {
                client.command_sender.send(TrackerCommand::ClientDisconnected(id));
            }
        }
    }

    pub fn clients(&self) -> impl Iterator<Item=&TrackerClient> {
        self.clients.values().map(|client| &client.client)
    }
}

pub async fn handle_manager_client(tracker_manager: Arc<Mutex<TrackerManager>>,
                                   client: TcpStream,
                                   client_id_ref: Arc<AtomicU64>) -> tokio::io::Result<()> {
    let (mut client_reader, mut client_writer) = client.into_split();
    let (command_sender, mut command_receiver) = mpsc::unbounded_channel::<TrackerCommand>();

    tokio::spawn(async move {
        while let Some(command) = command_receiver.recv().await {
            if let Err(_) = command.send_command(&mut client_writer).await {
                break;
            }
        }
    });

    loop {
        match TrackerCommand::receive_command(&mut client_reader).await? {
            TrackerCommand::Connect(address) => {
                let (client_id, clients) = {
                    let mut tracker_manager_guard = tracker_manager.lock().unwrap();

                    let client_id = tracker_manager_guard.new_client(address, command_sender.clone());
                    client_id_ref.store(client_id.0, Ordering::SeqCst);

                    let clients = tracker_manager_guard
                        .clients()
                        .filter(|client| client.id != client_id)
                        .cloned()
                        .collect();

                    (client_id, clients)
                };

                command_sender.send(TrackerCommand::ConnectionInfo {
                    id: client_id,
                    clients
                }).map_err(|_| tokio::io::Error::from(ErrorKind::Other))?;
            }
            TrackerCommand::ConnectionInfo { .. } => { eprintln!("Unexpected."); }
            TrackerCommand::ClientConnected(_) => { eprintln!("Unexpected."); }
            TrackerCommand::ClientDisconnected(_) => { eprintln!("Unexpected."); }
        }
    }
}

pub async fn run_client(tracker_address: SocketAddr,
                        folder: PathBuf) -> tokio::io::Result<()> {
    let mut tracker_client = TcpStream::connect(tracker_address).await?;

    let file_sync_manager = Arc::new(FileSyncManager::new(folder));
    let sync_listener_addr = start_sync_server(file_sync_manager.clone()).await;
    TrackerCommand::Connect(sync_listener_addr).send_command(&mut tracker_client).await?;

    file_sync_manager.start_background_tasks();

    loop {
        let command = TrackerCommand::receive_command(&mut tracker_client).await?;
        println!("{:#?}", command);

        match command {
            TrackerCommand::Connect(_) => { eprintln!("Unexpected."); }
            TrackerCommand::ConnectionInfo { id, clients } => {
                file_sync_manager.set_client_id(id);

                for client in clients {
                    if let Err(err) = start_sync_client(client, file_sync_manager.clone(), true).await {
                        println!("Sync client error: {:?}", err);
                    }
                }
            }
            TrackerCommand::ClientConnected(client) => {
                if let Err(err) = start_sync_client(client, file_sync_manager.clone(), false).await {
                    println!("Sync client error: {:?}", err);
                }
            }
            TrackerCommand::ClientDisconnected(client_id) => {
                file_sync_manager.remove_client(client_id);
            }
        }
    }
}

async fn start_sync_server(file_sync_manager: Arc<FileSyncManager>) -> SocketAddr {
    let sync_listener = try_create_listener().await;
    let sync_listener_addr = sync_listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            if let Ok((sync_client, sync_client_addr)) = sync_listener.accept().await {
                println!("Client connected: {}", sync_client_addr);

                let file_sync_manager_clone = file_sync_manager.clone();
                tokio::spawn(async move {
                    let channel_id = file_sync_manager_clone.next_commands_channel_id();
                    if let Err(result) = file_sync_manager_clone.run(sync_client,
                                                                     channel_id,
                                                                     mpsc::unbounded_channel(),
                                                                     false).await {
                        println!("Sync client error: {:?}", result);
                        file_sync_manager_clone.remove_active_requests(channel_id);
                    }
                });
            } else {
                break;
            }
        }
    });

    sync_listener_addr
}

async fn start_sync_client(client: TrackerClient,
                           file_sync_manager: Arc<FileSyncManager>,
                           sync: bool) -> tokio::io::Result<()> {
    let sync_client = TcpStream::connect(client.address).await?;

    tokio::spawn(async move {
        let commands_channel = mpsc::unbounded_channel();
        let channel_id = file_sync_manager.next_commands_channel_id();
        file_sync_manager.add_client(client.id, channel_id, commands_channel.0.clone());
        if let Err(result) = file_sync_manager.run(sync_client,
                                                   channel_id,
                                                   commands_channel,
                                                   sync).await {
            println!("Sync client error: {:?}", result);
            file_sync_manager.remove_active_requests(channel_id);
        }
    });

    Ok(())
}

async fn try_create_listener() -> TcpListener {
    let mut port = 8081;
    loop {
        if let Ok(listener) = TcpListener::bind(format!("127.0.0.1:{}", port)).await {
            return listener;
        }

        port += 1;
    }
}