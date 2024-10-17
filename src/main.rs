use clap::Parser;
use futures::prelude::*;
use limecached::LimeCachedShim;
use raft::{NodeState, RaftConsensus, RaftStates};
use rand::Rng;
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

#[derive(Parser, Debug, Clone)]
struct Flags {
    /// This is the unique ID per node
    #[clap(long)]
    node_id: u16,

    /// Sets the port number to listen on.
    #[clap(long)]
    port: u16,

    /// Sets the port number of the leader
    #[clap(long)]
    leader_port: u16,

    /// Decides if the current node is launching as a leader node or not
    #[clap(long)]
    bootstrap: bool,

    /// The Ports of all the peer nodes
    #[clap(long, value_delimiter = ',')]
    peer_ports: Vec<u16>,
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse the command line argument
    let election_timeout_ms: i32 = rand::thread_rng().gen_range(150..300);

    let flags = Flags::parse();
    println!("LAUNCHING RAFT SERVER | {:?}", flags);

    let server_addr = (IpAddr::V4(Ipv4Addr::LOCALHOST), flags.port);
    let leader_addr = (IpAddr::V4(Ipv4Addr::LOCALHOST), flags.leader_port);

    let mut module =
        limecached::LimecachedNode::new(flags.node_id, flags.bootstrap, election_timeout_ms);

    let locked_module: Arc<Mutex<limecached::LimecachedNode>> = Arc::new(Mutex::new(module));
    let locked_module_clone = Arc::clone(&locked_module);

    // Initialize the randomized timer that will trigger a leader election if
    // it isn't cancelled.
    let election_timer_jh = locked_module
        .lock()
        .unwrap()
        .initiate_heartbeat(election_timeout_ms);

    // Initialize taRPC Server
    let server_addr_clone = server_addr.clone();
    let flags_clone = flags.clone();

    let server_jh = tokio::task::spawn(async move {
        let listener = tarpc::serde_transport::tcp::listen(server_addr_clone, Json::default)
            .await
            .unwrap();
        println!("NODE {}: Listening for messages ...", flags_clone.port);
        listener
            // Ignore connection errors
            .filter_map(|r| future::ready(r.ok()))
            // Why is this necessary?
            .map(server::BaseChannel::with_defaults)
            // One channel per port
            .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().port())
            // this is what we call when we get an incoming connection
            .map(|channel| {
                // By doing this, we are dereferencing the mutexguard inside
                // the scope of execution, thus letting it live
                let shim = LimeCachedShim {
                    node_ref: Arc::clone(&locked_module),
                };
                channel.execute(shim.serve()).for_each(spawn)
            })
            .buffer_unordered(10)
            .for_each(|_| async {})
            .await;
    });

    // Register all the peers
    locked_module_clone
        .lock()
        .unwrap()
        .register_peers(flags.peer_ports)
        .await?;

    server_jh.await.unwrap();
    election_timer_jh.await.unwrap();

    Ok(())
}
