use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::net::{TcpListener};

use filesync::p2p;
use filesync::p2p::{P2PManager, ClientId};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tracker_listener = TcpListener::bind("127.0.0.1:8080").await?;

    let p2p_manager_main = Arc::new(Mutex::new(P2PManager::new()));

    loop {
        let (client, _) = tracker_listener.accept().await?;

        let p2p_manager = p2p_manager_main.clone();
        tokio::spawn(async move {
            let client_id = Arc::new(AtomicU64::new(0));

            if let Err(_) = p2p::handle_manager_client(p2p_manager.clone(), client, client_id.clone()).await {
                let client_id = client_id.load(Ordering::SeqCst);
                if client_id != 0 {
                    p2p_manager.lock().unwrap().remove_client(ClientId(client_id));
                }
            }
        });
    }
}