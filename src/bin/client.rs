use std::path::{PathBuf};
use std::str::FromStr;
use std::net::{SocketAddr, SocketAddrV4};

use filesync::p2p;

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    let folder = PathBuf::from_str(&std::env::args().skip(1).next().unwrap()).unwrap();
    println!("{}", folder.to_str().unwrap());

    let tracker_address = SocketAddr::from(SocketAddrV4::from_str("127.0.0.1:8080").unwrap());
    p2p::run_client(tracker_address, folder).await?;

    Ok(())
}
