use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, Mutex},
};

use clap::Parser;
use futures::prelude::*;
use stream::StreamExt;
use tarpc::{
    client, context,
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Json,
};

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

// Node-wide state
#[derive(Clone)]
enum RaftStates {
    // In this case, we are listening to entries from the leader and figuring out what to do
    Follower,
    // In this case, we are the leader, and we are responsible for replicating all the information
    // that we've recieved from the client.
    Leader,
    // In this case, we are a candidate, issuing election RPCs and waiting for votes
    Candidate,
}

#[tarpc::service]
trait RaftNode {
    async fn echo(message: String) -> String;

    // async fn append_entries();
    // async fn request_vote();
}

// This is the consensus module
#[derive(Clone)]
struct ConsensusModule {
    // Add the consensus state
    current_state: RaftStates,
    // This is the list of all the nodes in the cluster
    pub peers: Vec<RaftNodeClient>,
    // Add a log store
}

impl RaftNode for ConsensusModule {
    // This is what we need to fill in
    async fn echo(self, _: context::Context, message: String) -> String {
        println!("Recieved Request ...");
        format!("Echo: {message}")
    }
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

// This is the server, that accepts commands from the
pub struct Server {
    // Handle route: set commands
    // Handle route: get command
    // Handle route: appendEntries
    // Handle route: requestVote
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

    // Initialize the module. This isn't in the README
    let module = ConsensusModule {
        current_state: RaftStates::Follower,
        peers: Vec::new(),
    };

    let locked_module: Arc<Mutex<ConsensusModule>>;
    locked_module = Arc::new(Mutex::new(module));

    // This is the listen loop
    // every node starts its own raft server
    if flags.bootstrap {
        // Initialize our server node
        // This serde-transport thing is a very nifty abstraction
        let listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?;
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

                channel
                    // By doing this, we are dereferencing the mutexguard inside
                    // the scope of execution, thus letting it live
                    .execute(unlocked_server.to_owned().serve())
                    .for_each(spawn)
            })
            .buffer_unordered(10)
            .for_each(|_| async {})
            .await;
    }

    // if we are not the leader initially, do more
    let jh = tokio::spawn(async move {
        if !flags.bootstrap {
            println!("Executing after await");

            // Setup the TCP transport for the communication with the server
            let mut transport = tarpc::serde_transport::tcp::connect(leader_addr, Json::default);
            transport.config_mut().max_frame_length(usize::MAX);
            let client =
                RaftNodeClient::new(client::Config::default(), transport.await.unwrap()).spawn();

            let hello = client
                .echo(context::current(), format!("{}1", flags.port))
                .await;

            // let hello = async move {
            // // Send the request twice, just to be safe! ;)
            //     tokio::select! {
            //         hello1 = client.echo(context::current(), format!("{}1", flags.port)) => { hello1 }
            //         hello2 = client.echo(context::current(), format!("{}2", flags.port)) => { hello2 }
            //     }
            // }.await;

            match hello {
                Ok(hello) => println!("{hello:?}"),
                Err(e) => println!("{:?}", anyhow::Error::from(e)),
            }
        }
    });
    jh.await?;

    Ok(())
}
