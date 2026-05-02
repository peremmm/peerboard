# PeerBoard (Rust libp2p)

A distributed peer-to-peer message board built using:
- libp2p (TCP + QUIC + GossipSub + Kademlia + Rendezvous)
- protobuf (prost)
- SQLite (rusqlite)

---

## Features

- Peer discovery (Kademlia DHT)
- Message broadcasting (GossipSub)
- Protobuf message encoding
- Message validation (strict rules)
- SQLite storage
- CLI commands:
    - subscribe <topic>
    - unsubscribe <topic>
    - post <topic> <message>
    - list <topic>
    - discover
    - challenge <index>
      - shoot <column> <row>
      - resign
    - accept <true|false>
    - help

---

## Build

```bash
cargo build
```

---

## Running the system

### Option 1 — Two Terminals (Same Machine)

Terminal 1 (Subscriber)
```bash
cargo run -- --identity node1.key subscribe general
```

Terminal 2 (Publisher)
```bash
cargo run -- --identity node2.key publish general "hello world"
```

Expected

Terminal 1 should display:
```bash
Received post:
peer: ...
topic: peerboard/v1/general
content: hello world
```

### Option 2 — Two Different Machines

Make sure both machines:
* Have internet access
* Can reach the bootstrap node

Machine A (Subscriber)
```bash
cargo run -- subscribe general
```

Machine B (Publisher)
```bash
cargo run -- publish general "hello world"
```

Expected
Machine A receives the message after a few seconds.

---
### Suggested Test flow
1. Start subscriber
2. Start publisher
3. Verify message received
4. Run list to confirm storage
5. 

---

### Commands in Test
Run
```bash 
cargo run -- run 
```

Help - Displays the available commands. 

When you are not in a game, it shows message board and matchmaking commands.
When you are in a game, it only shows Battleship commands.
```bash 
help
```

Subscribe to a Topic
```bash 
subscribe general
```

Unsubscribe from a Topic
```bash 
unsubscribe general
```

Post a Message
```bash 
post general "hello world"
```

View Stored Messages
```bash 
view general
```

Discover Peers
```bash 
discover
```

Challenge a Peer
```bash 
challenge 1
```

Accept or Decline a Challenge
```bash 
accept true
```

Shoot - Fires at a coordinate during a Battleship game.
```bash 
shoot 0 0
```

Resign - Ends the current Battleship game and tells the opponent that you resigned.
```bash
resign
```
----
### Notes
Logs will be turned off during battle state. Turned on again after.

Identity Handling

Each node uses a unique identity file
```bash
(identity.key)
```
On the same machine → must use different files:
```bash
node1.key, node2.key
```
On different machines → default is fine

### Battleship

Ships are auto placed in fixed position

---
