use tarpc::context;

use crate::raft::{
    AppendEntriesRPCReq, AppendEntriesRPCRes, RaftConsensus, RaftStates, RequestVoteRPCReq,
    RequestVoteRPCRes,
};

// This is the consensus module
#[derive(Clone)]
pub struct LimecachedNode {
    // Add the consensus state
    pub current_state: RaftStates,
    // Add a log store
}

// Write node methods for handling actions on top of raft
// i.e. (in order of importance)
// 5. Running Heartbeats
// 4. Triggering elections
// 1. Writing to the store
// 2. Reading from the store
// 3. Snapshots(?) idk

impl RaftConsensus for LimecachedNode {
    // This is what we need to fill in
    async fn echo(self, _: context::Context, message: String) -> String {
        println!("Recieved Request ...");
        format!("Echo: {message}")
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
}
