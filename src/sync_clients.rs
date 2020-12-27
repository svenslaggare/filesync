use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::{HashMap, HashSet};

use rand::seq::SliceRandom;
use rand::thread_rng;

use crate::tracker::ClientId;
use crate::filesync::{ChannelId, SyncCommandsSender, SyncCommand};

pub struct ClientsManager {
    next_commands_channel_id: AtomicU64,
    clients_commands_sender: Mutex<HashMap<ClientId, (ChannelId, SyncCommandsSender)>>,
}

impl ClientsManager {
    pub fn new() -> ClientsManager {
        ClientsManager {
            clients_commands_sender: Mutex::new(HashMap::new()),
            next_commands_channel_id: AtomicU64::new(1),
        }
    }

    pub fn next_commands_channel_id(&self) -> ChannelId {
        ChannelId(self.next_commands_channel_id.fetch_add(1, Ordering::SeqCst))
    }

    pub fn add_client(&self, client_id: ClientId, channel_id: ChannelId, commands_sender: SyncCommandsSender) {
        self.clients_commands_sender.lock().unwrap().insert(client_id, (channel_id, commands_sender));
    }

    pub fn remove_client(&self, client_id: ClientId) {
        self.clients_commands_sender.lock().unwrap().remove(&client_id);
    }

    pub fn send_commands_all(&self, command: SyncCommand) {
        for (_, commands_sender) in self.clients_commands_sender.lock().unwrap().values() {
            if let Err(err) = commands_sender.send(command.clone()) {
                println!("{:?}", err);
            }
        }
    }

    pub fn send_commands_random_subset(&self, command: SyncCommand) -> usize {
        let clients_commands_sender_guard = self.clients_commands_sender.lock().unwrap();

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
        let clients_commands_sender_guard = self.clients_commands_sender.lock().unwrap();

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