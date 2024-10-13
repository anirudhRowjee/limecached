use clap::Parser;
use futures::prelude::*;
use raft::RaftConsensus;
use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, Mutex},
};
use stream::StreamExt;
use tarpc::{
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Json,
};

pub mod limecached;
pub mod raft;

#[derive(Parser)]
struct Flags {
    /// Sets the port number to listen on.
    #[clap(long)]
    port: u16,
    /// Sets the port number of the leader
    #[clap(long)]
    leader_port: u16,
    /// Decides if the current node is launching as a leader node or not
    #[clap(long)]
    bootstrap: bool,
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse the command line argument
    let flags = Flags::parse();
    println!(
        "LAUNCHING RAFT SERVER | PORT {} LEADER_PORT {} BOOTSTRAP {}",
        flags.port, flags.leader_port, flags.bootstrap
    );

    let server_addr = (IpAddr::V4(Ipv4Addr::LOCALHOST), flags.port);
    let leader_addr = (IpAddr::V4(Ipv4Addr::LOCALHOST), flags.leader_port);

    let module = limecached::LimecachedNode {
        current_state: raft::RaftStates::Follower,
    };
    let locked_module: Arc<Mutex<limecached::LimecachedNode>>;
    locked_module = Arc::new(Mutex::new(module));

    // Initialize our taRPC Server
    tokio::task::spawn(async move {
        let listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default)
            .await
            .unwrap();
        println!("NODE {}: Listening for messages ...", flags.port);
        listener
            // Ignore connection errors
            .filter_map(|r| future::ready(r.ok()))
            // Why is this necessary?
            .map(server::BaseChannel::with_defaults)
            // One channel per port
            .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().port())
            // this is what we call when we get an incoming connection
            .map(move |channel| {
                let local_server = Arc::clone(&locked_module);
                let unlocked_server = local_server.lock().unwrap();
                // By doing this, we are dereferencing the mutexguard inside
                // the scope of execution, thus letting it live
                channel
                    .execute(unlocked_server.to_owned().serve())
                    .for_each(spawn)
            })
            .buffer_unordered(10)
            .for_each(|_| async {})
            .await;
    });

    // Initialize the randomized timer

    // Initialize the HTTP Server to accept set and get commands

    Ok(())
}
