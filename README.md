# limecached 

limecached is an in-memory, raft-backed key-value store.

## Current state of the project

As of right now, the same binary can be used to send and recieve a single message via RPC.
Further work needs to be done to make sure each node has a running raft server, can track its peers, etc

## Usage Instructions

```bash
$ cargo build
```

open two terminals. On the first one,

```sh
$ cargo run -- --port=3000 --leader-port=3000 --bootstrap
...
LAUNCHING RAFT SERVER | PORT 3000 LEADER_PORT 3000 BOOTSTRAP true
NODE 3000: Listening for messages ...
```

on the second one,

```sh
$ cargo run -- --port=3001 --leader-port=3000
...
LAUNCHING RAFT SERVER | PORT 3001 LEADER_PORT 3000 BOOTSTRAP false
```

Now, you should see the second terminal do an RPC call on the leader - getting the expected echo reply

```sh
Executing after await
"Echo: 30011"
```

And on the side of the RPC server, you'll see:
```sh
Recieved Request ...
```
