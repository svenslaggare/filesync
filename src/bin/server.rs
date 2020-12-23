use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::net::{TcpListener};

use filesync::tracker;
use filesync::tracker::{TrackerManager, ClientId};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tracker_listener = TcpListener::bind("127.0.0.1:8080").await?;

    let tracker_manager_main = Arc::new(Mutex::new(TrackerManager::new()));

    loop {
        let (client, _) = tracker_listener.accept().await?;

        let tracker_manager = tracker_manager_main.clone();
        tokio::spawn(async move {
            let client_id = Arc::new(AtomicU64::new(0));

            if let Err(_) = tracker::handle_manager_client(tracker_manager.clone(), client, client_id.clone()).await {
                let client_id = client_id.load(Ordering::SeqCst);
                if client_id != 0 {
                    tracker_manager.lock().unwrap().remove_client(ClientId(client_id));
                }
            }
        });
    }
}