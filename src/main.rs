pub mod pb {
    // naming of the file is post-build generated
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

use libp2p::{
    identity,
    noise,
    tcp,
    quic,
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
use std::ptr::null;
use futures::StreamExt;
use prost::Message;
use pb::PeerBoardMessage;
use uuid::Uuid;
use rusqlite::{Connection, params};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, error};
use tracing_subscriber;
use tokio::time::{sleep, Duration};

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
    Subscribe {
        topic: String,
    },
    Publish {
        topic: String,
        message: String,
    },
    List,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let args = Args::parse();

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

    if let Some(cmd) = args.command {
        match cmd {
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

        match swarm.select_next_some().await {

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
                    info!("Identify received from {}", peer_id);

                    for addr in info.listen_addrs {
                        swarm
                            .behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, addr.clone());

                        info!("Added address for {}: {}", peer_id, addr);
                    }

                    if peer_id == bootstrap_peer_id && !bootstrap_done {
                        info!("Starting bootstrap AFTER identify");

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
            }

            SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(event)) => {
                match event {
                    KademliaEvent::OutboundQueryProgressed { result, .. } => {
                        match result {

                            QueryResult::Bootstrap(result) => {
                                if result.is_ok() {
                                    info!("Kademlia bootstrap completed");
                                }
                            }

                            QueryResult::GetClosestPeers(result) => {
                                match result {
                                    Ok(ok) => {
                                        info!("Closest peers:");
                                        for peer in ok.peers {
                                            println!("{:?}", peer);
                                        }
                                    }
                                    Err(e) => error!("GetClosestPeers error: {:?}", e),
                                }
                            }

                            _ => {}
                        }
                    }
                    _ => {}
                }
            }

            SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(event)) => {
                match event {
                    GossipsubEvent::Message {
                        propagation_source,
                        message_id: _,
                        message,
                    } => {
                        match pb::PeerBoardMessage::decode(&message.data[..]) {
                            Ok(msg) => {
                                if is_valid_and_new(&msg, &conn) {
                                    info!(
                                        "Received post:\n  peer: {}\n  topic: {}\n  content: {}\n  nickname: {}",
                                        msg.peer_id,
                                        msg.topic,
                                        msg.content,
                                        msg.nickname
                                    );
                                    insert_message_id(&msg, &conn);
                                }
                            }
                            Err(_) => {
                                // silent drop
                            }
                        }
                    }
                    _ => {}
                }
            }

            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                error!("Failed to connect to {:?}: {:?}", peer_id, error);
            }

            _ => {}
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