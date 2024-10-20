use core::time;
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4},
    sync::{Arc, Mutex},
};

use serde::ser::Error;
use tarpc::{client, context, tokio_serde::formats::Json};
use tokio::task::JoinSet;

use crate::raft::{
    self, AppendEntriesRPCReq, AppendEntriesRPCRes, LogEntry, NodeState, Peer, RaftConsensus,
    RaftConsensusClient, RaftStates, RequestVoteRPCReq, RequestVoteRPCRes,
};

#[derive(Clone)]
pub struct LimeCachedShim {
    pub node_ref: Arc<tokio::sync::Mutex<LimecachedNode>>,
}

// This is the consensus module
#[derive(Clone)]
pub struct LimecachedNode {
    // TODO see if we can move this into the trait definition
    // DO THIS LATER!!!!

    // All the raft state for this node
    own_port: u16,
    node_id: u16,
    pub current_state: RaftStates,
    raft_state: NodeState,
    logstore: Vec<LogEntry>,
    data: HashMap<String, String>,

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
    pub fn new(id: u16, own_port: u16, leader: bool, election_timeout_ms: i32) -> Self {
        LimecachedNode {
            node_id: id,
            own_port,
            current_state: raft::RaftStates::Follower,
            raft_state: NodeState {
                current_term: 0,
                voted_for: 0,
                log: Vec::new(),
                commit_index: 0,
                last_applied: 0,
                peers: HashMap::new(),
            },
            logstore: Vec::new(),
            data: HashMap::new(),
            election_timeout_ms,
        }
    }

    // Registers an external node as a peer
    pub async fn create_peer(&mut self, peer_port: u16, peer_id: u16) -> anyhow::Result<u16> {
        println!(
            "[ID:{}] Registering Peer with Port {} ID {}",
            self.node_id, peer_port, peer_id
        );
        let mut transport = tarpc::serde_transport::tcp::connect(
            SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), peer_port),
            Json::default,
        );
        transport.config_mut().max_frame_length(usize::MAX);
        let client = RaftConsensusClient::new(client::Config::default(), transport.await?).spawn();
        let peer = Peer {
            peer_id,
            peer_connection: client,
            next_index: 0,
            match_index: 0,
        };
        self.raft_state.peers.insert(peer_id, peer);
        Ok(peer_id)
    }

    pub async fn register_self_with_leader(&mut self, leader_id: u16) -> anyhow::Result<()> {
        let leader_ref = self.raft_state.peers.get(&leader_id).unwrap();
        leader_ref
            .peer_connection
            .add_peer(
                tarpc::context::current(),
                SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.own_port),
                self.node_id,
            )
            .await
            .unwrap();
        Ok(())
    }

    // Run an election
    pub async fn run_election(&mut self) {
        println!("[ID:{}] Running election", self.node_id);
        // Calculate the minimum number of votes we need
        let quorum_votes = self.raft_state.peers.len() / 2 + 1;
        // vote for ourselves
        let current_votes = 1;
        self.raft_state.current_term += 1;

        // Create a taskset
        let mut taskset: JoinSet<RequestVoteRPCRes> = tokio::task::JoinSet::new();
        let peers = self.raft_state.peers.clone();

        for (_, peer) in peers {
            let term = self.raft_state.current_term.clone();
            let candidate_id = self.node_id.clone();
            println!(
                "[ID:{}] Requesting Vote from Node {}",
                self.node_id, peer.peer_id
            );

            // Send the requestvote RPC
            taskset.spawn(async move {
                peer.peer_connection
                    .request_vote(
                        tarpc::context::current(),
                        RequestVoteRPCReq {
                            term,
                            candidate_id: candidate_id as i32,
                            last_log_term: 1,
                            last_log_index: 1,
                        },
                    )
                    .await
                    .unwrap()
            });
        }

        while let Some(k) = taskset.join_next().await {
            let res = k.unwrap();
            println!("Recieved Leader Election Response: {:#?}", res);
            // TODO Early exit when we
        }
    }

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
        let node_handle = self.node_ref.lock().await;
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
        let node_handle = self.node_ref.lock().await;
        println!("[ID:{}] recieved Request Vote RPC!", node_handle.node_id);
        RequestVoteRPCRes {
            node_id: node_handle.node_id,
            term: 1,
            vote_granted: true,
        }
    }

    // This function registers an external node as a peer
    // Ideally, new nodes will call this on the leader node (we are assuming that the leader node
    // port will always be known)
    async fn add_peer(
        self,
        _: tarpc::context::Context,
        peer_addr: SocketAddrV4,
        peer_id: u16,
    ) -> u16 {
        println!("Recieving add peer RPC!");
        let other_port = peer_addr.port();
        let mut node_handle = self.node_ref.lock().await;
        node_handle.create_peer(other_port, peer_id).await.unwrap();
        println!("Added other node as Peer!");
        return node_handle.node_id;
    }
}
