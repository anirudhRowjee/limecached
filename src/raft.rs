use std::{collections::HashMap, net::Ipv4Addr};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum LogEntry {
    Upsert(String, String),
    Delete(String),
    // TODO
    // AddPeer(ID, IPAddress)
    // RemovePeer(ID, IPAddress)
}

// Node-wide state
#[derive(Clone)]
pub enum RaftStates {
    // In this case, we are listening to entries from the leader and figuring out what to do
    Follower,
    // In this case, we are the leader, and we are responsible for replicating all the information
    // that we've recieved from the client.
    Leader,
    // In this case, we are a candidate, issuing election RPCs and waiting for votes
    Candidate,
}

// Information about each peer, including the connection to that peer,
// so that sending data to each peer is easy
#[derive(Debug, Clone)]
pub struct Peer {
    pub peer_id: u16,
    pub peer_connection: RaftConsensusClient,
    pub next_index: i32,
    pub match_index: i32,
}

// All the persistent and non-persistent state that each node needs to have
#[derive(Debug, Clone)]
pub struct NodeState {
    pub current_term: i32,
    pub voted_for: i32,
    pub log: Vec<LogEntry>,
    pub commit_index: i32,
    pub last_applied: i32,
    pub peers: HashMap<u16, Peer>,
}

/*
* RPC Requests and Responses
* Service Definition
*/

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AppendEntriesRPCReq {
    term: i32,
    leader_id: i32,
    prev_log_index: i32,
    prev_log_term: i32,
    entries: Vec<LogEntry>,
    leader_commit_index: i32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AppendEntriesRPCRes {
    term: i32,
    success: bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RequestVoteRPCReq {
    term: i32,
    candidate_id: i32,
    last_log_index: i32,
    last_log_term: i32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RequestVoteRPCRes {
    term: i32,
    vote_granted: bool,
}

#[tarpc::service]
pub trait RaftConsensus {
    // Return the ID of the node
    async fn echo() -> u16;
    async fn append_entries(req: AppendEntriesRPCReq) -> AppendEntriesRPCRes;
    async fn request_vote(req: RequestVoteRPCReq) -> RequestVoteRPCRes;
    async fn add_peer(ipaddr: Ipv4Addr) -> bool;
}
