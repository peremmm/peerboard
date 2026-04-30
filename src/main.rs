pub mod pb {
    // naming of the file is post-build generated
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

use libp2p::{
    identity,
    noise,
    tcp,
    yamux,
    Multiaddr,
    PeerId,
    swarm::{SwarmEvent, NetworkBehaviour},
    identify,
    kad::{self, store::MemoryStore, Behaviour as KademliaBehaviour, Event as KademliaEvent, QueryResult},
    gossipsub::{self, IdentTopic, MessageAuthenticity, Event as GossipsubEvent},
    StreamProtocol,
    SwarmBuilder,
};
use clap::{Parser, Subcommand};
use std::{
    error::Error,
    fs,
    path::Path,
};
use futures::StreamExt;
use prost::Message;
use pb::PeerBoardMessage;
use uuid::Uuid;
use rusqlite::{Connection, params};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, error};
use tracing_subscriber;
use tokio::{
    sync::mpsc,
    time::{Duration}
};

// Hardcoded bootstrap node
const BOOTSTRAP_ADDR: &str = "/ip4/170.64.177.57/tcp/8000/p2p/12D3KooWCvwqT3JUzVQczCvAVFa9EGzNqjHHSMVHVhm3RVyscCNY";

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    identify: identify::Behaviour,
    kademlia: KademliaBehaviour<MemoryStore>,
    gossipsub: gossipsub::Behaviour,
}

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "identity.key")]
    identity: String,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Run,
    Subscribe {
        topic: String,
    },
    Publish {
        topic: String,
        message: String,
    },
    List,
}

enum CliCommand {
    Subscribe(String),
    Unsubscribe(String),
    Publish(String, String),
    List,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let args = Args::parse();

    let is_run_mode = matches!(args.command, Some(Commands::Run));

    let mut bootstrap_done = false;

    let mut pending_publish: Option<(IdentTopic, Vec<u8>)> = None;

    let conn = Connection::open("messages.db")?;

    // logging purposes
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let keypair = load_or_create_identity(&args.identity)?;
    let peer_id = PeerId::from(keypair.public());

    info!("Peer ID: {}", peer_id);

    let mut swarm = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?.with_quic()
        .with_behaviour(|key| {
            let mut kad_config = kad::Config::new(
                StreamProtocol::new("/peerboard/kad/1.0.0")
            );

            let store = MemoryStore::new(key.public().to_peer_id());

            let kademlia = KademliaBehaviour::with_config(
                key.public().to_peer_id(),
                store,
                kad_config,
            );

            let gossipsub_config = gossipsub::Config::default();

            let gossipsub = gossipsub::Behaviour::new(
                MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            ).unwrap();

            Ok(MyBehaviour {
                identify: identify::Behaviour::new(
                    identify::Config::new(
                        "/peerboard/identify/1.0.0".into(),
                        key.public(),
                    )
                ),
                kademlia,
                gossipsub,
            })
        })?
        .with_swarm_config(|cfg| {
            cfg.with_idle_connection_timeout(Duration::from_secs(60))
        })
        .build();

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let bootstrap_addr: Multiaddr = BOOTSTRAP_ADDR.parse()?;
    let bootstrap_peer_id = extract_peer_id(&bootstrap_addr)?;

    swarm.dial(bootstrap_addr)?;
    info!("Dialing bootstrap node: {}", bootstrap_peer_id);

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

    if is_run_mode {
        let tx = cmd_tx.clone();

        tokio::spawn(async move {
            use tokio::io::{self, AsyncBufReadExt};

            let mut lines = io::BufReader::new(io::stdin()).lines();

            println!("Enter commands:");

            while let Ok(Some(line)) = lines.next_line().await {
                let parts: Vec<&str> = line.trim().split_whitespace().collect();

                if parts.is_empty() {
                    continue;
                }

                let cmd = match parts[0] {
                    "subscribe" if parts.len() == 2 => {
                        Some(CliCommand::Subscribe(parts[1].to_string()))
                    }
                    "unsubscribe" if parts.len() == 2 => {
                        Some(CliCommand::Unsubscribe(parts[1].to_string()))
                    }
                    "publish" if parts.len() >= 3 => {
                        let topic = parts[1].to_string();
                        let message = parts[2..].join(" ");
                        Some(CliCommand::Publish(topic, message))
                    }
                    "list" => Some(CliCommand::List),
                    _ => {
                        println!("Invalid command");
                        None
                    }
                };

                if let Some(cmd) = cmd {
                    let _ = tx.send(cmd);
                }
            }
        });
    }

    if !is_run_mode {
        if let Some(cmd) = args.command {
            match cmd {
                Commands::Run => {} // TODO

                Commands::Subscribe { topic } => {
                    let full_topic = format!("peerboard/v1/{}", topic);
                    let topic = IdentTopic::new(full_topic.clone());

                    swarm.behaviour_mut().gossipsub.subscribe(&topic).unwrap();
                    info!("Subscribed to {}", full_topic);
                }

                Commands::Publish { topic, message } => {
                    let full_topic = format!("peerboard/v1/{}", topic);
                    let topic = IdentTopic::new(full_topic.clone());

                    swarm.behaviour_mut().gossipsub.subscribe(&topic).unwrap();

                    let pb_msg = PeerBoardMessage {
                        peer_id: peer_id.to_string(),
                        topic: full_topic.clone(),
                        content: message.clone(),
                        timestamp: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs() as i64,
                        message_id: Uuid::new_v4().to_string(),
                        nickname: "emmanuel".to_string(),
                    };

                    if is_valid_and_new(&pb_msg, &conn) {
                        let mut buf = Vec::new();
                        pb_msg.encode(&mut buf).unwrap();

                        pending_publish = Some((topic, buf));
                        info!("Queued protobuf message for publishing");
                    }

                }

                Commands::List => {

                    let mut stmt = conn.prepare(
                        "SELECT peer_id, topic, content, timestamp, nickname
                        FROM messages
                        ORDER BY timestamp DESC"
                    ).unwrap();

                    let rows = stmt.query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    }).unwrap();

                    println!("\n------ Stored Messages ------");

                    for row in rows {
                        if let Ok((peer, topic, content, ts, nick)) = row {
                            println!(
                                "\n[{}] {} ({})\n{}\n",
                                topic, nick, peer, content
                            );
                        }
                    }

                    println!("------------------\n");
                }
            }
        }
    }

    conn.execute(
        "CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT PRIMARY KEY,
        peer_id TEXT NOT NULL,
        topic TEXT NOT NULL,
        content TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        nickname TEXT NOT NULL
    )",
        [],
    )?;

    // event loop magic
    loop {
        tokio::select! {

        Some(cmd) = cmd_rx.recv() => {
            match cmd {
                CliCommand::Subscribe(topic) => {
                    let full = format!("peerboard/v1/{}", topic);
                    let topic = IdentTopic::new(full.clone());

                    swarm.behaviour_mut().gossipsub.subscribe(&topic).unwrap();
                    println!("Subscribed to {}", full);
                }

                CliCommand::Unsubscribe(topic) => {
                    let full = format!("peerboard/v1/{}", topic);
                    let topic = IdentTopic::new(full.clone());

                    swarm.behaviour_mut().gossipsub.unsubscribe(&topic).unwrap();
                    println!("Unsubscribed from {}", full);
                }

                CliCommand::Publish(topic, message) => {
                    let full = format!("peerboard/v1/{}", topic);
                    let topic_obj = IdentTopic::new(full.clone());

                    swarm.behaviour_mut().gossipsub.subscribe(&topic_obj).unwrap();

                    let pb_msg = PeerBoardMessage {
                        peer_id: peer_id.to_string(),
                        topic: full.clone(),
                        content: message.clone(),
                        timestamp: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs() as i64,
                        message_id: Uuid::new_v4().to_string(),
                        nickname: "emmanuel".to_string(),
                    };

                    if is_valid_and_new(&pb_msg, &conn) {
                        let mut buf = Vec::new();
                        pb_msg.encode(&mut buf).unwrap();
                        pending_publish = Some((topic_obj, buf));
                    }
                }

                CliCommand::List => {
                    let mut stmt = conn.prepare(
                        "SELECT peer_id, topic, content, timestamp, nickname
                         FROM messages ORDER BY timestamp DESC"
                    ).unwrap();

                    let rows = stmt.query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    }).unwrap();

                    println!("\n------ Stored Messages ------");

                    for row in rows {
                        if let Ok((peer, topic, content, _, nick)) = row {
                            println!("[{}] {} ({})\n{}\n", topic, nick, peer, content);
                        }
                    }
                }
            }
        }

        event = swarm.select_next_some() => {
            match event {

                SwarmEvent::NewListenAddr { address, .. } => {
                    info!("Listening on {}", address);
                }

                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    info!("Connected to {}", peer_id);

                    if peer_id == bootstrap_peer_id && !bootstrap_done {
                        info!("Connected to bootstrap → starting DHT bootstrap");

                        if let Err(e) = swarm.behaviour_mut().kademlia.bootstrap() {
                            error!("Bootstrap failed: {:?}", e);
                        }

                        swarm
                            .behaviour_mut()
                            .kademlia
                            .get_closest_peers(peer_id);

                        bootstrap_done = true;
                    }
                }

                SwarmEvent::Behaviour(MyBehaviourEvent::Identify(event)) => {
                    if let identify::Event::Received { peer_id, info, .. } = event {
                        for addr in info.listen_addrs {
                            swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                        }
                    }
                }

                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(event)) => {
                    if let GossipsubEvent::Message { message, .. } = event {
                        if let Ok(msg) = pb::PeerBoardMessage::decode(&message.data[..]) {
                            if is_valid_and_new(&msg, &conn) {
                                info!("Received: {} -> {}", msg.topic, msg.content);
                                insert_message_id(&msg, &conn);
                            }
                        }
                    }
                }

                _ => {}
            }
        }
    }

        if let Some((topic, data)) = pending_publish.take() {
            let connected = swarm.connected_peers().count();

            let mesh = swarm
                .behaviour()
                .gossipsub
                .mesh_peers(&topic.hash())
                .count();

            if connected > 0 && mesh > 0 {
                match swarm.behaviour_mut().gossipsub.publish(topic, data) {
                    Ok(_) => info!("Published message"),
                    Err(_) => {}
                }
            } else {
                pending_publish = Some((topic, data));
            }
        }
    }
}

fn load_or_create_identity(path: &str) -> Result<identity::Keypair, Box<dyn Error>> {
    if Path::new(path).exists() {
        println!("Existing identity file found at {:?}", path);
        let bytes = fs::read(path)?;
        let keypair = identity::Keypair::from_protobuf_encoding(&bytes)?;
        Ok(keypair)
    } else {
        let keypair = identity::Keypair::generate_ed25519();
        let bytes = keypair.to_protobuf_encoding()?;
        println!("New identity file generated at {:?}", path);
        fs::write(path, bytes)?;
        Ok(keypair)
    }
}

// helper (private)
fn extract_peer_id(addr: &Multiaddr) -> Result<PeerId, Box<dyn Error>> {
    for protocol in addr.iter() {
        if let libp2p::multiaddr::Protocol::P2p(peer_id) = protocol {
            return Ok(peer_id);
        }
    }
    Err("No PeerId found in multiaddr".into())
}

fn is_valid_and_new(msg: &pb::PeerBoardMessage, conn: &Connection) -> bool {

    if !msg.topic.starts_with("peerboard/v1/") {
        return false;
    }

    if msg.content.as_bytes().len() > 4096 {
        return false;
    }

    if msg.nickname.as_bytes().len() > 32 {
        return false;
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    if msg.timestamp > now + 300 {
        return false;
    }

    if uuid::Uuid::parse_str(&msg.message_id).is_err() {
        return false;
    }

    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM messages WHERE message_id = ?1)",
        params![msg.message_id],
        |row| row.get(0),
    ).unwrap_or(false);

    if exists {
        return false;
    }

    true
}

fn insert_message_id(msg: &pb::PeerBoardMessage, conn: &Connection) {
    let _ = conn.execute(
        "INSERT INTO messages (
            message_id, peer_id, topic, content, timestamp, nickname
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            msg.message_id,
            msg.peer_id,
            msg.topic,
            msg.content,
            msg.timestamp,
            msg.nickname
        ],
    );
}