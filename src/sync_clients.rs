use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::{HashMap, HashSet};

use rand::seq::SliceRandom;
use rand::thread_rng;

use tokio::io::ErrorKind;

use crate::tracker::ClientId;
use crate::filesync::{ChannelId, SyncCommandsSender, SyncCommand};

pub struct PeersManager {
    next_commands_channel_id: AtomicU64,
    peers: Mutex<HashMap<ClientId, (ChannelId, SyncCommandsSender)>>,
}

impl PeersManager {
    pub fn new() -> PeersManager {
        PeersManager {
            peers: Mutex::new(HashMap::new()),
            next_commands_channel_id: AtomicU64::new(1),
        }
    }

    pub fn next_commands_channel_id(&self) -> ChannelId {
        ChannelId(self.next_commands_channel_id.fetch_add(1, Ordering::SeqCst))
    }

    pub fn add_peer(&self, client_id: ClientId, channel_id: ChannelId, commands_sender: SyncCommandsSender) {
        self.peers.lock().unwrap().insert(client_id, (channel_id, commands_sender));
    }

    pub fn remove_peer(&self, client_id: ClientId) {
        self.peers.lock().unwrap().remove(&client_id);
    }

    pub fn send_commands_all(&self, command: SyncCommand) {
        for (_, commands_sender) in self.peers.lock().unwrap().values() {
            if let Err(err) = commands_sender.send(command.clone()) {
                println!("{:?}", err);
            }
        }
    }

    pub fn send_commands_to(&self, client_id: ClientId, command: SyncCommand) -> tokio::io::Result<()> {
        if let Some(client) = self.peers.lock().unwrap().get(&client_id) {
            client.1.send(command).map_err(|_| tokio::io::Error::from(ErrorKind::Other))
        } else {
            Err(tokio::io::Error::from(ErrorKind::Other))
        }
    }

    pub fn send_commands_random_subset(&self, command: SyncCommand) -> usize {
        let clients_commands_sender_guard = self.peers.lock().unwrap();

        if !clients_commands_sender_guard.is_empty() {
            let mut keys = clients_commands_sender_guard.keys().collect::<Vec<_>>();
            keys.shuffle(&mut thread_rng());
            // let count = thread_rng().gen_range(1..(keys.len() + 1));
            let count = 1;

            let mut used_count = 0;
            for client in &keys {
                if clients_commands_sender_guard[*client].1.send(command.clone()).is_ok() {
                    used_count += 1;
                }

                if used_count >= count {
                    break;
                }
            }

            used_count
        } else {
            0
        }
    }

    pub fn send_command_random(&self, command: SyncCommand, exclude_channels: &HashSet<ChannelId>) -> bool {
        let clients_commands_sender_guard = self.peers.lock().unwrap();

        if !clients_commands_sender_guard.is_empty() {
            let mut keys = clients_commands_sender_guard.keys().collect::<Vec<_>>();
            keys.shuffle(&mut thread_rng());

            for key in keys {
                let (channel_id, commands_sender) = &clients_commands_sender_guard[key];
                if !exclude_channels.contains(channel_id) {
                    if commands_sender.send(command.clone()).is_ok() {
                        return true;
                    }
                }
            }

            false
        } else {
            false
        }
    }
}