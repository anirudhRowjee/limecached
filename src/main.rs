use clap::Parser;
use futures::prelude::*;
use limecached::LimeCachedShim;
use raft::{NodeState, RaftConsensus, RaftStates};
use rand::Rng;
use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Duration,
};
use stream::StreamExt;
use tarpc::{
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Json,
};
use tokio::sync::Mutex;

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

    /// Sets the peer ID of the leader
    #[clap(long)]
    leader_id: u16,

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
    let election_timeout_ms: i32 = rand::thread_rng().gen_range(150..300);

    // Parse the command line arguments
    let flags = Flags::parse();
    println!("LAUNCHING RAFT SERVER | {:?}", flags);

    let server_addr = (IpAddr::V4(Ipv4Addr::LOCALHOST), flags.port);

    let module = limecached::LimecachedNode::new(
        flags.node_id,
        flags.port,
        flags.bootstrap,
        election_timeout_ms,
    );

    let locked_module: Arc<tokio::sync::Mutex<limecached::LimecachedNode>> =
        Arc::new(Mutex::new(module));
    let locked_module_clone = Arc::clone(&locked_module);
    let locked_module_timer = Arc::clone(&locked_module);

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
                    node_ref: Arc::clone(&locked_module_clone),
                };
                channel.execute(shim.serve()).for_each(spawn)
            })
            .buffer_unordered(10)
            .for_each(|_| async {})
            .await;
    });

    // If we aren't a leader
    if !flags.bootstrap {
        tokio::task::spawn(async move {
            println!("Current node is not a leader!");
            let mut handle = locked_module.lock().await;
            // Register ourselves as a peer on the leader
            handle
                .create_peer(flags.leader_port, flags.leader_id)
                .await
                .unwrap();
            handle
                .register_self_with_leader(flags.leader_id)
                .await
                .unwrap();
            println!("Registered leader as peer!");
            // wait until the election takes place
        })
        .await
        .unwrap();
    } else {
        // /shrug this will have to do for now, it takes me this long to start all nodes via the
        // terminal ;-;
        // TODO Move this into the heartbeat/election timeout thread
        tokio::time::sleep(Duration::from_secs(10)).await;
        let mut handle = locked_module.lock().await;
        handle.run_election().await;
    }

    // Create channels to recieve and send durations
    let (mut sender, mut reciever) =
        tokio::sync::mpsc::unbounded_channel::<tokio::time::Duration>();

    let election_jh = tokio::task::spawn(async move {
        let mut election_interval =
            tokio::time::interval(Duration::from_millis(election_timeout_ms as u64));
        let mut heartbeat_interval = tokio::time::interval(Duration::from_millis(10));

        // Initialize the timer in a loop
        loop {
            tokio::select! {

                _ = election_interval.tick() => {
                    // check for leadership under a scoped lock so we don't contend
                    let is_leader: bool = {
                        let node_handle = locked_module_timer.lock().await;
                        node_handle.raft_state.is_leader
                    };
                    if !is_leader {
                        println!("Initiating Leader Election");
                        let mut node_handle = locked_module_timer.lock().await;
                        node_handle.run_election().await;
                    }
                }

                _ = heartbeat_interval.tick() => {
                    // check for leadership under a scoped lock so we don't contend
                    let is_leader: bool = {
                        let node_handle = locked_module_timer.lock().await;
                        node_handle.raft_state.is_leader
                    };
                    if is_leader {
                        let mut node_handle = locked_module_timer.lock().await;
                        println!("Sending Heartbeat");
                        node_handle.send_heartbeat().await;
                    }
                }

                // TODO use this later to change the interval and the default action
                // when the node state changes
                _ = reciever.recv() => {}
            }
        }
    });

    server_jh.await.unwrap();
    election_jh.await.unwrap();

    Ok(())
}
