# Limecached 

limecached is an in-memory, raft-backed key-value store.

## The State of the Project

Right now, both the `AppendEntries` and the `RequestVote` RPC are in place. There is a heartbeat timer, as well as an election timer, that runs at a decided interval. In the steady state, the nodes will start, a leader will be elected, and the leader will happily keep replicating log entries to the follower nodes. 

If the leader is killed, one of the nodes will convert itself to leader first, and then initiate an election. However, given that the nodes don't know about each other, it can't ask anyone for a vote. This is because of the current design of the peer discovery mechanism.

```
   A
  / \
 B-!-C
```

## Usage Instructions

```bash
$ cargo build
```



## Getting started

Open three terminals.

On the leader terminal,
```
$ cargo run -- --node-id=1 --port=3000 --leader-port=3000 --bootstrap --leader-id=1
```
And on the other two terminals,
```
$ cargo run -- --node-id=2 --port=3001 --leader-port=3000 --leader-id=1 
$ cargo run -- --node-id=3 --port=3002 --leader-port=3000 --leader-id=1 
```

This will cause the follower nodes to register themselves with the leader node as peers.

Leader - 
```
LAUNCHING RAFT SERVER | Flags { node_id: 1, port: 3000, leader_port: 3000, leader_id: 1, bootstrap: true, peer_ports: [] }
NODE 3000: Listening for messages ...
Recieving add peer RPC!
[ID:1] Registering Peer with Port 3001 ID 2
Added other node as Peer!
Recieving add peer RPC!
[ID:1] Registering Peer with Port 3002 ID 3
Added other node as Peer!
```

Other nodes - 
```
# node 2
LAUNCHING RAFT SERVER | Flags { node_id: 2, port: 3001, leader_port: 3000, leader_id: 1, bootstrap: false, peer_ports: [] }
Current node is not a leader!
[ID:2] Registering Peer with Port 3000 ID 1
NODE 3001: Listening for messages ...
Registered leader as peer!
  
# node 3
LAUNCHING RAFT SERVER | Flags { node_id: 3, port: 3002, leader_port: 3000, leader_id: 1, bootstrap: false, peer_ports: [] }
Current node is not a leader!
[ID:3] Registering Peer with Port 3000 ID 1
NODE 3002: Listening for messages ...
Registered leader as peer!
```

## Leader election
After about 10 seconds, the leader should initiate a leader election. 

```
[ID:1] Running election
[ID:1] Requesting Vote from Node 3
[ID:1] Requesting Vote from Node 2
Recieved Leader Election Response: RequestVoteRPCRes {
    node_id: 3,
    term: 1,
    vote_granted: true,
}
Recieved Leader Election Response: RequestVoteRPCRes {
    node_id: 2,
    term: 1,
    vote_granted: true,
}
```

After this, the leader will keep replicating log entries to the follower nodes.
