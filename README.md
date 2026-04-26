# PeerBoard (Rust libp2p)

A distributed peer-to-peer message board built using:
- libp2p (TCP + QUIC + GossipSub + Kademlia)
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
    - subscribe
    - publish
    - list

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
2. Wait 10–20 seconds
3. Start publisher
4. Verify message received
5. Run list to confirm storage

---

### Notes
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

---

### Common Issues
💔 No message received

* Ensure different PeerIds
* Wait ~10–20 seconds for mesh formation

💔 Stuck in pending

Happens if no peers in GossipSub mesh yet
System will retry automatically

---

### Commands
Subscribe
```bash cargo run -- subscribe <topic> ```

Publish
```bash cargo run -- publish <topic> "<message>" ```

List Stored Messages (on any device/terminal)
```bash cargo run -- list```
