use core::time;
use std::{
    collections::{BTreeMap, HashMap},
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
};

use anyhow::Error;
use tarpc::{client, context, tokio_serde::formats::Json};
use tokio::task::JoinSet;

use crate::{
    raft::{
        self, AppendEntriesRPCReq, AppendEntriesRPCRes, LogEntry, LogType, NodeState, Peer,
        RaftConsensus, RaftConsensusClient, RaftStates, RequestVoteRPCReq, RequestVoteRPCRes,
    },
    DurationAction,
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
    timer_event_sender: tokio::sync::mpsc::UnboundedSender<DurationAction>,
    election_timeout_ms: i32,
}

struct LogStore {
    // backing log store
    store: Vec<LogEntry>,
    // total size of all the entries
    length: Option<u64>,
    // index until which the log has been committed
    commit_index: Option<u64>,
}

// Write node methods for handling actions on top of raft
// i.e. (in order of importance)
// 5. Running Heartbeats
// 4. Triggering elections
// 1. Writing to the store
// 2. Reading from the store
// 3. Snapshots(?) idk
impl LimecachedNode {
    pub fn new(
        id: u16,
        own_port: u16,
        leader: bool,
        election_timeout_ms: i32,
        timer_sender: tokio::sync::mpsc::UnboundedSender<DurationAction>,
    ) -> Self {
        LimecachedNode {
            node_id: id,
            own_port,
            current_state: raft::RaftStates::Follower,
            raft_state: NodeState {
                is_leader: leader,
                current_term: 0,
                voted_for: 0,
                // We keep this at 0 because the code is supposed to be 1-indexed
                log: Vec::new(),
                commit_index: 0,
                last_applied: 0,
                peers: HashMap::new(),
            },
            logstore: vec![LogEntry {
                entry: LogType::Sentinel,
                term: 0,
                index: 0,
            }],
            data: BTreeMap::new(),
            election_timeout_ms,
            timer_event_sender: timer_sender,
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
        if self.raft_state.is_leader {
            self.raft_state.last_applied += 1;
            let new_log_entry = LogEntry {
                entry: LogType::AddPeer(peer_port, peer_id),
                term: self.raft_state.current_term as u16,
                index: self.raft_state.last_applied as u16,
            };
            self.logstore.push(new_log_entry);
        }

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

        self.timer_event_sender
            .send(DurationAction::ResetElectionTimer)
            .unwrap();

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
        // TODO Figure out which data to send to which node based on the current
        // forwarded state

        self.timer_event_sender
            .send(DurationAction::ResetElectionTimer)
            .unwrap();

        let mut taskset: JoinSet<AppendEntriesRPCRes> = tokio::task::JoinSet::new();
        let peers = self.raft_state.peers.clone();

        for (peer_id, peer) in peers {
            let term = self.raft_state.current_term.clone();
            let candidate_id = self.node_id.clone();
            let last_log_term = self.raft_state.current_term.clone();
            let last_log_index = self.raft_state.commit_index.clone();

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
        }
        while let Some(k) = taskset.join_next().await {
            let res = k.unwrap();
            println!("Recieved AppendEntries Response: {:#?}", res);
        }
    }

    // Write to all the other nodes
    pub async fn write(&mut self, key: String, value: String) -> anyhow::Result<String, Error> {
        let node_id = self.node_id;
        println!("[ID:{node_id}] Recieved Frontend Write (key, value) = ({key}, {value})");

        // TODO if we are a leader, do the following else, make RPC call on leader
        //
        // find the term and index of the latest log entry
        let mut last_log_term = 0;
        let mut last_log_index = 0;
        match self.logstore.last() {
            Some(log_entry) => match log_entry.entry {
                // Ignore the sentinel
                LogType::Sentinel => {}
                _ => {
                    last_log_term = log_entry.term;
                    last_log_index = log_entry.index;
                }
            },
            // The very first message will be this;
            None => {
                // Inherit the term of the node
                last_log_term = self.raft_state.current_term as u16;
            }
        }

        // 1. add a log entry
        self.raft_state.last_applied += 1;
        let new_log_entry = LogEntry {
            entry: LogType::Upsert(key.clone(), value.clone()),
            term: self.raft_state.current_term as u16,
            index: self.raft_state.last_applied as u16,
        };
        // This entry should be at self.logstore[last_applied + 1]
        self.logstore.push(new_log_entry);

        let replication_start_idx = self.raft_state.commit_index + 1;
        let replication_end_idx = self.raft_state.last_applied;
        let mut entries_to_be_replicated = Vec::new();
        for x in replication_start_idx..=replication_end_idx {
            let local_entry = self.logstore.get(x as usize).unwrap();
            entries_to_be_replicated.push(local_entry.clone());
        }
        println!("Replicating batch => {:#?}", entries_to_be_replicated);

        // 2. initiate replication via AppendEntries (is there some sort of disconnect here? how do
        //    other nodes know that there's new state here?)
        let quorum_count = self.raft_state.peers.len() / 2 + 1;
        let mut taskset: JoinSet<AppendEntriesRPCRes> = tokio::task::JoinSet::new();
        let peers = self.raft_state.peers.clone();

        for (peer_id, peer) in peers {
            // Track data to verify our invariants
            let term = self.raft_state.current_term.clone();
            let candidate_id = self.node_id.clone();
            let current_last_log_term = last_log_term.clone();
            let current_last_log_index = last_log_index.clone();

            let last_commit_index = self.raft_state.commit_index.clone();

            // See how we can make this
            let entries = entries_to_be_replicated.clone();

            println!(
                "[ID:{}] Replicating Write to Node {}",
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
                            prev_log_index: current_last_log_index as i32,
                            prev_log_term: current_last_log_term as i32,
                            entries,
                            leader_commit_index: last_commit_index,
                        },
                    )
                    .await
                    .unwrap()
            });
        }

        let mut accepted_count = 1;
        // 3. Confirm quorum replication, exit early when possible
        while let Some(k) = taskset.join_next().await {
            let res = k.unwrap();
            println!("Recieved AppendEntries(Write) Response: {:#?}", res);
            if res.success {
                accepted_count += 1;
                if accepted_count >= quorum_count {
                    println!("[ID:{}] Quorum replication Confirmed!", self.node_id);
                    break;
                }
            }
        }
        // TODO what if we Don't have quorum replication?

        // 2. Commit on local and update last_committed index
        self.raft_state.commit_index += entries_to_be_replicated.len() as i32;
        // We are assuming that the rest of the data has already been persisted if it's in the log
        println!("Committing to local state => {key} | {value}");
        self.data.insert(key, value.clone());

        // 4. return to user
        return Ok(value);
    }

    // read from the local state
    pub fn read(&mut self, key: String) -> anyhow::Result<String, Error> {
        match self.data.get(&key) {
            Some(value) => Ok(value.to_owned()),
            None => Err(anyhow::Error::msg("could not")),
        }
    }
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
        if req.entries.len() == 0 {
            println!("Heartbeat from leader recieved!")
        } else {
            println!("New Entries from leader recieved!")
        }

        let mut node_handle = self.node_ref.lock().await;
        node_handle
            .timer_event_sender
            .send(DurationAction::ResetElectionTimer)
            .unwrap();

        // 5.1 - check that the terms match
        if req.term < node_handle.raft_state.current_term {
            println!("Term Mismatch!");
            return AppendEntriesRPCRes {
                term: node_handle.raft_state.current_term as i32,
                success: false,
            };
        }

        // If this is not a heartbeat,
        if req.entries.len() != 0 {
            // Check that the latest log entires exist and are maintained
            let mut ok = true;
            // If there is no entry in the log yet, then we can't check this
            println!("DEBUG CHECK OF LOG : {:#?}", node_handle.logstore);
            if req.prev_log_index != 0 {
                match node_handle.logstore.get(req.prev_log_index as usize) {
                    Some(entry) => {
                        // If the terms and the index match, we're good
                        // the guarantee is that we have the same data at this point
                        if entry.term as i32 != req.prev_log_term {
                            ok = false;
                            unimplemented!("TODO handle conflicting terms");
                        } else {
                            ok = true;
                        }
                    }
                    None => {
                        println!("No entry at the back of the log!");
                        // ok = false;
                    }
                }
            }

            if !ok {
                println!("No matching entries!");
                return AppendEntriesRPCRes {
                    term: node_handle.raft_state.current_term as i32,
                    success: false,
                };
            }

            // append all new entries to the log
            let mut entries = req.entries.clone();
            println!("Appending to log {:#?}", entries);
            let mut last_new_index = 0;

            node_handle.logstore.append(&mut entries);
            last_new_index = node_handle.logstore.last().unwrap().index;

            // See if we need to commit any entries from the log into local storage
            println!("Checking to see if there are uncommitted entries");
            if req.leader_commit_index > node_handle.raft_state.last_applied {
                println!("Beginning write process...");
                // Compute the new commit index
                let new_commit_index =
                    std::cmp::min(req.leader_commit_index, last_new_index as i32);
                println!("new committ index {new_commit_index}");

                let new_writes: bool = node_handle.raft_state.commit_index < new_commit_index;
                println!(
                    "new writes verdict -> {new_writes} | local CI  {} computed CI {}",
                    node_handle.raft_state.commit_index, new_commit_index
                );
                if new_writes {
                    for i in node_handle.raft_state.commit_index + 1..=new_commit_index {
                        // Edge case: handle the case where the 0th entry doesn't get committed
                        let entry = node_handle.logstore.get(i as usize).unwrap().clone();
                        println!("Committing Log Entry [Index: {i}]=> {:#?}", entry);

                        // actually write the entry
                        match &entry.entry {
                            LogType::Upsert(k, v) => {
                                node_handle.data.insert(k.clone(), v.clone());
                            }
                            LogType::Delete(k) => {
                                unimplemented!("Have not implemented Deletes yet");
                            }
                            LogType::AddPeer(port, id) => {
                                println!("Adding Peers with port {port} and ID {id}");
                                node_handle.create_peer(*port, *id).await.unwrap();
                            }
                            _ => {}
                        }
                    }
                    node_handle.raft_state.commit_index = new_commit_index;
                }
            }
            println!("Finished Checking to see if there are uncommitted entries");
        }

        AppendEntriesRPCRes {
            term: node_handle.raft_state.current_term as i32,
            success: true,
        }
    }

    async fn request_vote(
        self,
        context: ::tarpc::context::Context,
        req: RequestVoteRPCReq,
    ) -> RequestVoteRPCRes {
        let mut decision = false;
        let mut node_handle = self.node_ref.lock().await;
        let own_id = node_handle.node_id;
        let candidate_id = req.candidate_id;
        println!("[ID:{}] recieved Request Vote RPC!", node_handle.node_id);

        // Checking if the term is less than the current term (old node trying to become leader)
        if req.term < node_handle.raft_state.current_term {
            decision = false;
        }
        // Check if we have voted for this node (or nobody else) before
        let vote_check = node_handle.raft_state.voted_for == candidate_id
            || node_handle.raft_state.voted_for == 0;
        // AND
        // log is at least as up to date as our log
        let log_freshness_check = req.last_log_term >= node_handle.raft_state.current_term
            && req.last_log_index >= node_handle.raft_state.last_applied;
        // grant the vote
        if vote_check && log_freshness_check {
            decision = true;
            node_handle.raft_state.voted_for = candidate_id;
        }

        println!("[ID:{own_id}] Responding To RequestVote RPC from node {candidate_id} with response {decision}");

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
        // TODO Persist this information to the replicated log
        println!("Added other node as Peer!");
        return node_handle.node_id;
    }
}
