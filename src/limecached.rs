use core::time;
use std::{
    collections::{BTreeMap, HashMap},
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
};

use anyhow::Error;
use tarpc::{client, context, tokio_serde::formats::Json};
use tokio::task::JoinSet;

use crate::raft::{
    self, AppendEntriesRPCReq, AppendEntriesRPCRes, LogEntry, LogType, NodeState, Peer,
    RaftConsensus, RaftConsensusClient, RaftStates, RequestVoteRPCReq, RequestVoteRPCRes,
};

#[derive(Clone)]
pub struct LimeCachedShim {
    pub node_ref: Arc<tokio::sync::Mutex<LimecachedNode>>,
}

// This is the consensus module
#[derive(Clone)]
pub struct LimecachedNode {
    // All the raft state for this node
    own_port: u16,
    node_id: u16,
    pub current_state: RaftStates,
    pub raft_state: NodeState,
    logstore: Vec<LogEntry>,
    data: BTreeMap<String, String>,

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
                is_leader: leader,
                current_term: 0,
                voted_for: 0,
                log: Vec::new(),
                commit_index: 0,
                last_applied: 0,
                peers: HashMap::new(),
            },
            logstore: Vec::new(),
            data: BTreeMap::new(),
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
        // TODO insert this into the replicated log only if we are the leader
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
        let mut current_votes = 1;
        // Increment the current term
        self.raft_state.current_term += 1;

        // Create a taskset
        let mut taskset: JoinSet<RequestVoteRPCRes> = tokio::task::JoinSet::new();
        let peers = self.raft_state.peers.clone();

        for (_, peer) in peers {
            let term = self.raft_state.current_term.clone();
            let candidate_id = self.node_id.clone();
            let last_log_term = self.raft_state.current_term.clone();
            let last_log_index = self.raft_state.last_applied.clone();

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
                            last_log_term,
                            last_log_index,
                        },
                    )
                    .await
                    .unwrap()
            });
        }

        while let Some(k) = taskset.join_next().await {
            let res = k.unwrap();
            println!("Recieved Leader Election Response: {:#?}", res);
            current_votes += {
                if res.vote_granted {
                    1
                } else {
                    0
                }
            };
            if current_votes == quorum_votes {
                println!("Look at me! I am the leader now.");
                self.raft_state.is_leader = true;
                self.current_state = RaftStates::Leader;
            }
        }

        // IF we're the leader, send out empty heartbeat messages to prevent
        // follower election timers from going off
        self.send_heartbeat().await;
    }

    // Function to call appendEntries on all nodes in parallel; abstracting this
    // because both the heartbeat and the log replication step use this
    pub async fn send_heartbeat(&mut self) {
        let mut taskset: JoinSet<AppendEntriesRPCRes> = tokio::task::JoinSet::new();
        let peers = self.raft_state.peers.clone();

        for (_, peer) in peers {
            let term = self.raft_state.current_term.clone();
            let candidate_id = self.node_id.clone();
            let last_log_term = self.raft_state.current_term.clone();
            let last_log_index = self.raft_state.last_applied.clone();

            println!(
                "[ID:{}] Appending Entries to Node {}",
                self.node_id, peer.peer_id
            );

            // Send the requestvote RPC
            taskset.spawn(async move {
                peer.peer_connection
                    .append_entries(
                        tarpc::context::current(),
                        AppendEntriesRPCReq {
                            leader_id: candidate_id as i32,
                            term,
                            prev_log_index: last_log_index,
                            prev_log_term: last_log_term,
                            entries: vec![],
                            leader_commit_index: 0,
                        },
                    )
                    .await
                    .unwrap()
            });

            while let Some(k) = taskset.join_next().await {
                let res = k.unwrap();
                println!("Recieved AppendEntries Response: {:#?}", res);
            }
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
    pub async fn write(&mut self, key: String, value: String) -> anyhow::Result<String, Error> {
        // if we are a leader, do the following
        // else, make RPC call on leader

        // 1. add a log entry
        let new_log_entry = LogEntry {
            entry: LogType::Upsert(key.clone(), value.clone()),
            term: self.raft_state.current_term as u16,
            index: self.raft_state.last_applied as u16 + 1,
        };
        self.logstore.push(new_log_entry);

        // 2. initiate replication via AppendEntries (is there some sort of disconnect here? how do
        //    other nodes know that there's new state here?)
        // 3. Confirm quorum replication, exit early when possible

        // 2. Commit on local and update last_committed index
        self.data.insert(key, value.clone()).unwrap();
        self.raft_state.last_applied += 1;

        // 4. return to user
        return Ok(value);
    }

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
        println!("Recieved Append Entries Request: {:#?}", req);
        let node_handle = self.node_ref.lock().await;
        if (req.entries.len() == 0) {
            println!("Heartbeat from leader recieved!")
        } else {
            println!("New Entries from leader recieved!")
        }
        AppendEntriesRPCRes {
            term: node_handle.node_id as i32,
            success: true,
        }
    }

    async fn request_vote(
        self,
        context: ::tarpc::context::Context,
        req: RequestVoteRPCReq,
    ) -> RequestVoteRPCRes {
        let mut decision = false;
        let node_handle = self.node_ref.lock().await;
        println!("[ID:{}] recieved Request Vote RPC!", node_handle.node_id);

        // Checking if the term is less than the current term (old node trying to become leader)
        if req.term < node_handle.raft_state.current_term {
            decision = false;
        }
        // Check if we have voted for this node (or nobody else) before
        let vote_check = node_handle.raft_state.voted_for == req.candidate_id
            || node_handle.raft_state.voted_for == 0;
        // AND
        // log is at least as up to date as our log
        let log_freshness_check = req.last_log_term >= node_handle.raft_state.current_term
            && req.last_log_index >= node_handle.raft_state.last_applied;
        // grant the vote
        if vote_check && log_freshness_check {
            decision = true;
        }

        RequestVoteRPCRes {
            node_id: node_handle.node_id,
            term: node_handle.raft_state.current_term,
            vote_granted: decision,
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
