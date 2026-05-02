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
    rendezvous::{self, Namespace},
    request_response::{self, ProtocolSupport, Behaviour as RequestResponseBehaviour, Codec, Message as RequestResponseMessage},
    StreamProtocol,
    SwarmBuilder,
};
use clap::{Parser, Subcommand};
use std::{
    collections::HashSet,
    error::Error,
    io,
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH}
};
use futures::{prelude::*, StreamExt};
use prost::Message;
use pb::PeerBoardMessage;
use uuid::Uuid;
use rusqlite::{Connection, params};
use tracing::{info, error};
use tracing_subscriber::{fmt, EnvFilter, reload, prelude::*};
use tokio::{
    sync::mpsc,
    time::{Duration}
};
use tracing_subscriber::reload::Handle;

// Hardcoded bootstrap node
const BOOTSTRAP_ADDR: &str = "/ip4/170.64.177.57/tcp/8000/p2p/12D3KooWCvwqT3JUzVQczCvAVFa9EGzNqjHHSMVHVhm3RVyscCNY";

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    identify: identify::Behaviour,
    kademlia: KademliaBehaviour<MemoryStore>,
    gossipsub: gossipsub::Behaviour,
    rendezvous: rendezvous::client::Behaviour,
    challenge: RequestResponseBehaviour<ChallengeCodec>,
    battleship: RequestResponseBehaviour<BattleshipCodec>,
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
}

enum CliCommand {
    Subscribe(String),
    Unsubscribe(String),
    Post(String, String),
    View(String),
    Discover,
    Challenge(usize),
    Help,
}

#[derive(Clone)]
struct ChallengeProtocol;

impl AsRef<str> for ChallengeProtocol {
    fn as_ref(&self) -> &str {
        "/peerboard/challenge/1.0.0"
    }
}

#[derive(Debug, Clone)]
struct ChallengeRequest {
    nickname: String,
}

#[derive(Debug, Clone)]
struct ChallengeResponseMsg {
    accepted: bool,
}

#[derive(Clone, Default)]
struct ChallengeCodec;

#[async_trait::async_trait]
impl Codec for ChallengeCodec {
    type Protocol = ChallengeProtocol;
    type Request = ChallengeRequest;
    type Response = ChallengeResponseMsg;

    async fn read_request<T>(&mut self, _: &ChallengeProtocol, io: &mut T)
                             -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;

        let msg = pb::ChallengePropose::decode(&buf[..])
            .map_err(|_| io::ErrorKind::InvalidData)?;

        Ok(ChallengeRequest {
            nickname: msg.nickname,
        })
    }

    async fn read_response<T>(&mut self, _: &ChallengeProtocol, io: &mut T)
                              -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;

        let msg = pb::ChallengeResponse::decode(&buf[..])
            .map_err(|_| io::ErrorKind::InvalidData)?;

        Ok(ChallengeResponseMsg {
            accepted: msg.accepted,
        })
    }

    async fn write_request<T>(&mut self, _: &ChallengeProtocol, io: &mut T, req: ChallengeRequest)
                              -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let msg = pb::ChallengePropose {
            nickname: req.nickname,
        };

        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        io.write_all(&buf).await?;
        io.close().await?;
        Ok(())
    }

    async fn write_response<T>(&mut self, _: &ChallengeProtocol, io: &mut T, res: ChallengeResponseMsg)
                               -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let msg = pb::ChallengeResponse {
            accepted: res.accepted,
        };

        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        io.write_all(&buf).await?;
        io.close().await?;
        Ok(())
    }
}

#[derive(Clone)]
struct BattleshipProtocol;

impl AsRef<str> for BattleshipProtocol {
    fn as_ref(&self) -> &str {
        "/peerboard/battleship/1.0.0"
    }
}

#[derive(Debug, Clone)]
struct BattleshipReq;

#[derive(Debug, Clone)]
struct BattleshipRes;

#[derive(Clone, Default)]
struct BattleshipCodec;

#[async_trait::async_trait]
impl Codec for BattleshipCodec {
    type Protocol = BattleshipProtocol;
    type Request = BattleshipReq;
    type Response = BattleshipRes;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T)
                             -> std::io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;

        let _ = pb::BattleshipRequest::decode(&buf[..])
            .map_err(|_| std::io::ErrorKind::InvalidData)?;

        Ok(BattleshipReq)
    }

    async fn read_response<T>(&mut self, _: &Self::Protocol, io: &mut T)
                              -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;

        let _ = pb::BattleshipResponse::decode(&buf[..])
            .map_err(|_| std::io::ErrorKind::InvalidData)?;

        Ok(BattleshipRes)
    }

    async fn write_request<T>(&mut self, _: &Self::Protocol, io: &mut T, _: Self::Request)
                              -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let msg = pb::BattleshipRequest {
            msg: Some(pb::battleship_request::Msg::BoardReady(pb::BoardReady {})),
        };

        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        io.write_all(&buf).await?;
        io.close().await?;
        Ok(())
    }

    async fn write_response<T>(&mut self, _: &Self::Protocol, io: &mut T, _: Self::Response)
                               -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let msg = pb::BattleshipResponse {
            msg: Some(pb::battleship_response::Msg::BoardAck(pb::BoardAck {})),
        };

        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        io.write_all(&buf).await?;
        io.close().await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let args = Args::parse();

    let is_run_mode = matches!(args.command, Some(Commands::Run));

    let mut bootstrap_done = false;

    let mut known_peers: HashSet<PeerId> = HashSet::new();
    let mut discovered_peers: HashSet<PeerId> = HashSet::new();
    let mut selected_peer: Option<PeerId> = None;

    let mut in_game = false;
    let mut is_my_turn = false;

    let mut pending_publish: Option<(IdentTopic, Vec<u8>)> = None;

    let conn = Connection::open("messages.db")?;

    // logging purposes
    let (filter_layer, handle) = reload::Layer::new(EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt::layer())
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

            let cfg = request_response::Config::default()
                .with_request_timeout(Duration::from_secs(30));

            let protocols = std::iter::once((
                ChallengeProtocol,
                ProtocolSupport::Full,
            ));

            let challenge = RequestResponseBehaviour::new(
                protocols,
                cfg,
            );

            let battleship_cfg = request_response::Config::default()
                .with_request_timeout(Duration::from_secs(30));

            let battleship_protocols = std::iter::once((
                BattleshipProtocol,
                ProtocolSupport::Full,
            ));

            let battleship = RequestResponseBehaviour::new(
                battleship_protocols,
                battleship_cfg,
            );

            Ok(MyBehaviour {
                identify: identify::Behaviour::new(
                    identify::Config::new(
                        "/peerboard/identify/1.0.0".into(),
                        key.public(),
                    )
                ),
                kademlia,
                gossipsub,
                rendezvous: rendezvous::client::Behaviour::new(key.clone()),
                challenge,
                battleship
            })
        })?
        .with_swarm_config(|cfg| {
            cfg.with_idle_connection_timeout(Duration::from_secs(60))
        })
        .build();

    let listen_addr: Multiaddr = "/ip4/0.0.0.0/tcp/0".parse()?;
    swarm.listen_on(listen_addr.clone())?;
    swarm.add_external_address(listen_addr);

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

            print_help();

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
                    "post" if parts.len() >= 3 => {
                        let topic = parts[1].to_string();
                        let message = parts[2..].join(" ");
                        Some(CliCommand::Post(topic, message))
                    }
                    "view" if parts.len() >= 2 => {
                        Some(CliCommand::View(parts[1].to_string()))
                    },
                    "discover" => Some(CliCommand::Discover),
                    "challenge" if parts.len() == 2 => {
                        if let Ok(idx) = parts[1].parse::<usize>() {
                            Some(CliCommand::Challenge(idx))
                        } else {
                            println!("Invalid index");
                            None
                        }
                    }
                    "help" => Some(CliCommand::Help),
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
                    if is_valid_topic(&topic) {
                        let full = format!("peerboard/v1/{}", topic);
                        let topic = IdentTopic::new(full.clone());

                        swarm.behaviour_mut().gossipsub.subscribe(&topic).unwrap();
                        println!("Subscribed to {}", full);
                    }
                }

                CliCommand::Unsubscribe(topic) => {
                    let full = format!("peerboard/v1/{}", topic);
                    let topic = IdentTopic::new(full.clone());

                    swarm.behaviour_mut().gossipsub.unsubscribe(&topic).unwrap();
                    println!("Unsubscribed from {}", full);
                }

                CliCommand::Post(topic, message) => {
                    if is_valid_topic(&topic) {
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
                }

                CliCommand::View(topic) => {
                    if !is_valid_topic(&topic) {
                        return Err("Invalid topic".into());
                    }
                    let full_topic = format!("peerboard/v1/{}", topic);

                    let mut stmt = conn.prepare(
                        "SELECT peer_id, topic, content, timestamp, nickname
                         FROM messages
                         WHERE topic = ?1
                         ORDER BY timestamp DESC"
                    )?;

                    let rows = stmt.query_map([&full_topic], |row| {
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

                CliCommand::Discover => {
                    use rendezvous::Namespace;

                    let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();
                    swarm.behaviour_mut().rendezvous.discover(Some(ns), None, None, bootstrap_peer_id);
                }

                CliCommand::Challenge(index) => {
                    let peers: Vec<_> = discovered_peers.iter().cloned().collect();

                    if index == 0 || index > peers.len() {
                        println!("Invalid selection");
                        continue;
                    }
                    let target = peers[index - 1];
                    selected_peer = Some(target);
                    println!("Sending challenge to: {}", target);

                    swarm.behaviour_mut().challenge.send_request(
                        &target,
                        ChallengeRequest {
                            nickname: "emmanuel".to_string(),
                        },
                    );
                }

                CliCommand::Help => {
                    print_help();
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
                }

                SwarmEvent::Behaviour(MyBehaviourEvent::Identify(event)) => {
                    if let identify::Event::Received { peer_id, info, .. } = event {
                        for addr in info.listen_addrs {
                            swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                        }

                        if peer_id == bootstrap_peer_id && !bootstrap_done {
                            info!("Identify received from bootstrap → starting DHT bootstrap");

                            if let Err(e) = swarm.behaviour_mut().kademlia.bootstrap() {
                                error!("Bootstrap failed: {:?}", e);
                            }
                            // self lookup
                            swarm
                                .behaviour_mut()
                                .kademlia
                                .get_closest_peers(PeerId::from(keypair.public()));

                            bootstrap_done = true;
                        }

                        if !in_game {
                            let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();
                            if let Err(e) = swarm
                                .behaviour_mut()
                                .rendezvous
                                .register(ns, bootstrap_peer_id, None)
                            {
                                error!("Rendezvous register failed (retry): {:?}", e);
                            } else {
                                info!("Rendezvous register retried");
                            }
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

                SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(event)) => {
                    match event {
                        KademliaEvent::OutboundQueryProgressed { result, .. } => {
                            match result {
                                QueryResult::GetClosestPeers(Ok(ok)) => {

                                    for peer in ok.peers {
                                        known_peers.insert(peer.peer_id);
                                    }
                                    info!("Known peers: {}", known_peers.len());

                                    if known_peers.len() < 3 {
                                        info!("Peer count < 3 → re-bootstrapping");

                                        if let Err(e) = swarm.behaviour_mut().kademlia.bootstrap() {
                                            error!("Re-bootstrap failed: {:?}", e);
                                        }
                                    }
                                }
                                    QueryResult::GetClosestPeers(Err(e)) => {
                                        error!("GetClosestPeers error: {:?}", e);
                                    }
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }

                    SwarmEvent::Behaviour(MyBehaviourEvent::Rendezvous(event)) => {
                        match event {
                            rendezvous::client::Event::Registered { namespace, .. } => {
                                info!("Successfully registered in namespace: {}", namespace);
                            }
                            rendezvous::client::Event::RegisterFailed { namespace, error, .. } => {
                                error!("Register failed for {}: {:?}", namespace, error);
                            }
                            rendezvous::client::Event::Discovered { registrations, .. } => {
                                discovered_peers.clear();

                                for reg in registrations {
                                    discovered_peers.insert(reg.record.peer_id());
                                }

                                println!("\nDiscovered peers:");

                                let peers: Vec<_> = discovered_peers.iter().cloned().collect();

                                for (i, peer) in peers.iter().enumerate() {
                                    println!("{}. {}", i + 1, peer);
                                }

                                println!("\n[peerboard] > ");
                            }
                            _ => {}
                        }
                    }

                    SwarmEvent::Behaviour(MyBehaviourEvent::Challenge(event)) => {
                        match event {
                            request_response::Event::Message { peer, message } => {
                                match message {
                                    RequestResponseMessage::Request {
                                        request,
                                        channel,
                                        ..
                                    } => {
                                        println!("Incoming challenge from {} (nickname: {})", peer, request.nickname);

                                        let response = ChallengeResponseMsg {
                                            accepted: true,
                                        };

                                        swarm.behaviour_mut().challenge.send_response(channel, response).unwrap();

                                        println!("Accepted challenge from {}", peer);
                                        println!("Game starting with {}", peer);
                                        is_my_turn = false;
                                        // println!("Opponent goes first");

                                        in_game = true;
                                        toggle_logs_during_in_game(in_game, &handle);
                                        if let Some(target) = selected_peer {
                                            println!("Sending BoardReady to {}", target);
                                            swarm.behaviour_mut().battleship.send_request(&target, BattleshipReq);
                                        }

                                        // Unregister
                                        use rendezvous::Namespace;
                                        let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();

                                        swarm.behaviour_mut().rendezvous.unregister(ns, bootstrap_peer_id);
                                        println!("Unregistered from matchmaking");
                                    }

                                    RequestResponseMessage::Response {
                                        response,
                                        ..
                                    } => {
                                        println!("Challenge response: accepted = {}", response.accepted);

                                        if response.accepted {
                                            is_my_turn = true;
                                            println!("Game starting with {}", peer);

                                            in_game = true;
                                            toggle_logs_during_in_game(in_game, &handle);
                                            if let Some(target) = selected_peer {
                                                println!("Sending BoardReady to {}", target);
                                                swarm.behaviour_mut().battleship.send_request(&target, BattleshipReq);
                                            }

                                            // Unregister from rendezvous
                                            use rendezvous::Namespace;
                                            let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();

                                            swarm.behaviour_mut().rendezvous.unregister(ns, bootstrap_peer_id);
                                            println!("Unregistered from matchmaking");
                                        }
                                    }
                                }
                            }

                            request_response::Event::OutboundFailure { peer, error, .. } => {
                                println!("Challenge outbound failure to {}: {:?}", peer, error);
                            }

                            request_response::Event::InboundFailure { peer, error, .. } => {
                                println!("Challenge inbound failure from {}: {:?}", peer, error);
                            }

                            request_response::Event::ResponseSent { peer, .. } => {
                                println!("Response sent to {}", peer);
                            }
                        }
                    }

                    SwarmEvent::Behaviour(MyBehaviourEvent::Battleship(event)) => {
                        match event {
                            request_response::Event::Message { peer, message } => {
                                match message {
                                    RequestResponseMessage::Request { channel, .. } => {
                                        is_my_turn = false; // opponent waits
                                        println!("Received BoardReady from {}", peer);
                                        swarm.behaviour_mut().battleship.send_response(channel, BattleshipRes).unwrap();
                                        println!("Sent BoardAck to {}", peer);
                                        println!(
                                            "Game ready. {}",
                                            if is_my_turn { "Your turn" } else { "Waiting for opponent" }
                                        );
                                    }
                                    RequestResponseMessage::Response { .. } => {
                                        println!("Received BoardAck from {}", peer);
                                        println!(
                                            "Game ready. {}",
                                            if is_my_turn { "Your turn" } else { "Waiting for opponent" }
                                        );
                                    }
                                }
                            }
                            _ => {}
                        }
                    }

                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        known_peers.remove(&peer_id);
                        info!("Peer disconnected. Known peers: {}", known_peers.len());
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

fn print_help() {
    println!("\nAvailable commands:");
    println!("subscribe <topic>");
    println!("unsubscribe <topic>");
    println!("post <topic> <message>");
    println!("view <topic>");
    println!("discover");
    println!("challenge <index>");
    println!("help");
    println!("\n[peerboard] > ")
}

fn is_valid_topic(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

fn toggle_logs_during_in_game<S>(b: bool, handle: &reload::Handle<EnvFilter, S>, ) {
    let level = if b { "warn" } else { "info" };
    handle.reload(EnvFilter::new(level)).unwrap();
}