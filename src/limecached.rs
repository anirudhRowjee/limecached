use core::time;
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4},
    sync::{Arc, Mutex},
};

use serde::ser::Error;
use tarpc::{client, context, tokio_serde::formats::Json};

use crate::raft::{
    self, AppendEntriesRPCReq, AppendEntriesRPCRes, LogEntry, NodeState, Peer, RaftConsensus,
    RaftConsensusClient, RaftStates, RequestVoteRPCReq, RequestVoteRPCRes,
};

#[derive(Clone)]
pub struct LimeCachedShim {
    pub node_ref: Arc<Mutex<LimecachedNode>>,
}

// This is the consensus module
#[derive(Clone)]
pub struct LimecachedNode {
    // TODO see if we can move this into the trait definition
    // DO THIS LATER!!!!

    // All the raft state for this node
    node_id: u16,
    pub current_state: RaftStates,
    raft_state: Arc<Mutex<NodeState>>,
    logstore: Arc<Mutex<Vec<LogEntry>>>,
    data: Arc<Mutex<HashMap<String, String>>>,

    election_timeout_ms: i32,
}

// Write node methods for handling actions on top of raft
// i.e. (in order of importance)
// 5. Running Heartbeats
// 4. Triggering elections
// 1. Writing to the store
// 2. Reading from the store
// 3. Snapshots(?) idk
impl LimecachedNode {
    pub fn new(id: u16, leader: bool, election_timeout_ms: i32) -> Self {
        LimecachedNode {
            node_id: id,
            current_state: raft::RaftStates::Follower,
            raft_state: Arc::new(Mutex::new(NodeState {
                current_term: 0,
                voted_for: 0,
                log: Vec::new(),
                commit_index: 0,
                last_applied: 0,
                peers: HashMap::new(),
            })),
            logstore: Arc::new(Mutex::new(Vec::new())),
            data: Arc::new(Mutex::new(HashMap::new())),
            election_timeout_ms,
        }
    }

    pub async fn register_peers(&self, peer_ports: Vec<u16>) -> anyhow::Result<()> {
        // Accept the peer ports
        for port in peer_ports {
            let mut transport = tarpc::serde_transport::tcp::connect(
                SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port),
                Json::default,
            );
            transport.config_mut().max_frame_length(usize::MAX);
            let client =
                RaftConsensusClient::new(client::Config::default(), transport.await?).spawn();
            let node_id = client.echo(context::current()).await?;
            println!("recieved Echo RPC Response from Node {}", node_id);

            // Update the node state
            let mut state_handle = self.raft_state.lock().unwrap();
            let _ = state_handle.peers.insert(
                node_id,
                Peer {
                    peer_id: node_id,
                    peer_connection: client,
                    next_index: 0,
                    match_index: 0,
                },
            );
        }
        return anyhow::Ok(());
    }

    pub fn run_election() {}

    // Spawn this on a separate thread, outside of the tokio runtime
    pub fn initiate_heartbeat(&self, election_duration_ms: i32) -> tokio::task::JoinHandle<()> {
        let interval = election_duration_ms.clone();
        let id = self.node_id.clone();
        let a = tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(time::Duration::from_millis(interval as u64));
            loop {
                interval.tick().await;
                println!("[ID:{}] Initiating Leader Election", id);
            }
        });
        return a;
    }

    // Spawn this on a separate thread, outside of the tokio runtime
    pub fn initiate_election_timeout() {}

    // Write to all the other nodes
    // pub fn write(key: String, value: String) -> Result<String, Error> {}

    pub fn read() {}
}

impl RaftConsensus for LimeCachedShim {
    // This is what we need to fill in
    async fn echo(self, _: context::Context) -> u16 {
        let node_handle = self.node_ref.lock().unwrap();
        println!("[ID:{}] Recieved Echo ...", node_handle.node_id);
        return node_handle.node_id;
    }

    async fn append_entries(
        self,
        context: ::tarpc::context::Context,
        req: AppendEntriesRPCReq,
    ) -> AppendEntriesRPCRes {
        todo!()
    }

    async fn request_vote(
        self,
        context: ::tarpc::context::Context,
        req: RequestVoteRPCReq,
    ) -> RequestVoteRPCRes {
        todo!()
    }

    async fn add_peer(self, context: ::tarpc::context::Context, ipaddr: Ipv4Addr) -> bool {
        todo!()
    }
}
