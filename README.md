# limecached 

limecached is an in-memory, raft-backed key-value store.

## Current state of the project

As of right now, you can start multiple processes with this setup as below. 
The nodes can discover and talk to each other based on the ports provided in the command line options, for a simple echo RPC.

## Usage Instructions

```bash
$ cargo build
```

open two terminals. On the first one,

```sh
$ cargo run -- --node-id=1 --port=3000 --leader-port=3000 --bootstrap
...
LAUNCHING RAFT SERVER | PORT 3000 LEADER_PORT 3000 BOOTSTRAP true
NODE 3000: Listening for messages ...
```

on two other terminals (this is a race at this point, need to fix)
```sh
$ cargo run -- --node-id=2 --port=3001 --leader-port=3000 --peer-ports=3000,3002
$ cargo run -- --node-id=3 --port=3002 --leader-port=3000 --peer-ports=3001,3002
```

You should see the leader election timeout start on the leader node, and you should see, on the leader node:
```
Recieved Echo...
```
