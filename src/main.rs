pub mod pb {
    // naming of the file is post-build generated
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

use clap::{Parser, Subcommand};
use futures::{StreamExt, prelude::*};
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, SwarmBuilder,
    gossipsub::{self, Event as GossipsubEvent, IdentTopic, MessageAuthenticity},
    identify, identity,
    kad::{
        self, Behaviour as KademliaBehaviour, Event as KademliaEvent, QueryResult,
        store::MemoryStore,
    },
    noise,
    rendezvous::{self, Namespace},
    request_response::{
        self, Behaviour as RequestResponseBehaviour, Codec, Message as RequestResponseMessage,
        ProtocolSupport,
    },
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use pb::PeerBoardMessage;
use prost::Message;
use rusqlite::{Connection, params};
use std::{
    collections::HashSet,
    error::Error,
    fs,
    io::{self, IsTerminal, Write},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{sync::mpsc, time::Duration};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt, prelude::*, reload};
use uuid::Uuid;

// Hardcoded bootstrap node
const BOOTSTRAP_ADDR: &str =
    "/ip4/170.64.177.57/tcp/8000/p2p/12D3KooWCvwqT3JUzVQczCvAVFa9EGzNqjHHSMVHVhm3RVyscCNY";

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
#[command(
    name = "peerboard",
    about = "P2P message board with Battleship matchmaking",
    version
)]
struct Args {
    #[arg(long, default_value = "identity.key", help = "Path to your local peer identity file")]
    identity: String,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Start the interactive PeerBoard CLI")]
    Run,
}

enum CliCommand {
    Subscribe(String),
    Unsubscribe(String),
    Post(String, String),
    View(String),
    Discover,
    Challenge(usize),
    Accept(bool),
    Shoot(u32, u32),
    Resign,
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

    async fn read_request<T>(
        &mut self,
        _: &ChallengeProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;

        let msg = pb::ChallengePropose::decode(&buf[..]).map_err(|_| io::ErrorKind::InvalidData)?;

        Ok(ChallengeRequest {
            nickname: msg.nickname,
        })
    }

    async fn read_response<T>(
        &mut self,
        _: &ChallengeProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;

        let msg =
            pb::ChallengeResponse::decode(&buf[..]).map_err(|_| io::ErrorKind::InvalidData)?;

        Ok(ChallengeResponseMsg {
            accepted: msg.accepted,
        })
    }

    async fn write_request<T>(
        &mut self,
        _: &ChallengeProtocol,
        io: &mut T,
        req: ChallengeRequest,
    ) -> io::Result<()>
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

    async fn write_response<T>(
        &mut self,
        _: &ChallengeProtocol,
        io: &mut T,
        res: ChallengeResponseMsg,
    ) -> io::Result<()>
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
struct BattleshipReq {
    msg: pb::BattleshipRequest,
}

#[derive(Debug, Clone)]
struct BattleshipRes {
    msg: pb::BattleshipResponse,
}

#[derive(Clone, Default)]
struct BattleshipCodec;

#[async_trait::async_trait]
impl Codec for BattleshipCodec {
    type Protocol = BattleshipProtocol;
    type Request = BattleshipReq;
    type Response = BattleshipRes;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;

        let msg =
            pb::BattleshipRequest::decode(&buf[..]).map_err(|_| std::io::ErrorKind::InvalidData)?;

        Ok(BattleshipReq { msg })
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.read_to_end(&mut buf).await?;

        let msg = pb::BattleshipResponse::decode(&buf[..])
            .map_err(|_| std::io::ErrorKind::InvalidData)?;

        Ok(BattleshipRes { msg })
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let mut buf = Vec::new();
        req.msg.encode(&mut buf).unwrap();

        io.write_all(&buf).await?;
        io.close().await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let mut buf = Vec::new();
        res.msg.encode(&mut buf).unwrap();

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

    let mut pending_challenge: Option<(
        PeerId,
        request_response::ResponseChannel<ChallengeResponseMsg>,
    )> = None;

    let mut in_game = false;
    let mut is_my_turn = false;
    let mut shot_seq: u32 = 1;

    let my_board = create_random_board();
    let mut my_hits = [[false; 10]; 10];
    let mut my_shots = [[false; 10]; 10];
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

    print_welcome(&peer_id);
    info!("Peer ID: {}", peer_id);

    let mut swarm = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|key| {
            let kad_config = kad::Config::new(StreamProtocol::new("/peerboard/kad/1.0.0"));

            let store = MemoryStore::new(key.public().to_peer_id());

            let kademlia =
                KademliaBehaviour::with_config(key.public().to_peer_id(), store, kad_config);

            let gossipsub_config = gossipsub::Config::default();

            let gossipsub = gossipsub::Behaviour::new(
                MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )
            .unwrap();

            let cfg =
                request_response::Config::default().with_request_timeout(Duration::from_secs(30));

            let protocols = std::iter::once((ChallengeProtocol, ProtocolSupport::Full));

            let challenge = RequestResponseBehaviour::new(protocols, cfg);

            let battleship_cfg =
                request_response::Config::default().with_request_timeout(Duration::from_secs(30));

            let battleship_protocols = std::iter::once((BattleshipProtocol, ProtocolSupport::Full));

            let battleship = RequestResponseBehaviour::new(battleship_protocols, battleship_cfg);

            Ok(MyBehaviour {
                identify: identify::Behaviour::new(identify::Config::new(
                    "/peerboard/identify/1.0.0".into(),
                    key.public(),
                )),
                kademlia,
                gossipsub,
                rendezvous: rendezvous::client::Behaviour::new(key.clone()),
                challenge,
                battleship,
            })
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
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

            print_help(in_game);

            while let Ok(Some(line)) = lines.next_line().await {
                let trimmed = line.trim();
                let parts: Vec<&str> = trimmed.split_whitespace().collect();

                if parts.is_empty() {
                    print_prompt();
                    continue;
                }

                let cmd = match parts[0] {
                    "subscribe" if parts.len() == 2 => {
                        Some(CliCommand::Subscribe(parts[1].to_string()))
                    }
                    "unsubscribe" if parts.len() == 2 => {
                        Some(CliCommand::Unsubscribe(parts[1].to_string()))
                    }
                    "post" | "publish" if parts.len() >= 3 => {
                        let topic = parts[1].to_string();
                        let message = trim_wrapping_quotes(&parts[2..].join(" ")).to_string();
                        Some(CliCommand::Post(topic, message))
                    }
                    "view" | "list" if parts.len() == 2 => {
                        Some(CliCommand::View(parts[1].to_string()))
                    }
                    "discover" | "peers" if parts.len() == 1 => Some(CliCommand::Discover),
                    "challenge" if parts.len() == 2 => {
                        if let Ok(idx) = parts[1].parse::<usize>() {
                            Some(CliCommand::Challenge(idx))
                        } else {
                            ui_error("Invalid peer index. Use: challenge <number>");
                            None
                        }
                    }
                    "accept" if parts.len() == 2 => match parse_yes_no(parts[1]) {
                        Some(value) => Some(CliCommand::Accept(value)),
                        None => {
                            ui_error("Use: accept true, accept false, accept yes, or accept no");
                            None
                        }
                    },
                    "shoot" if parts.len() == 3 => {
                        if let (Ok(col), Ok(row)) = (parts[1].parse(), parts[2].parse()) {
                            Some(CliCommand::Shoot(col, row))
                        } else {
                            ui_error("Invalid coordinates. Use: shoot <col 0-9> <row 0-9>");
                            None
                        }
                    }
                    "help" => Some(CliCommand::Help),
                    "resign" => Some(CliCommand::Resign),
                    _ => {
                        ui_error("Invalid command. Type 'help' to see what PeerBoard understands.");
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
                        if !is_valid_topic(&topic) {
                            ui_invalid_topic(&topic);
                            continue;
                        }

                        let full = format!("peerboard/v1/{}", topic);
                        let topic = IdentTopic::new(full.clone());

                        swarm.behaviour_mut().gossipsub.subscribe(&topic).unwrap();
                        ui_success(format!("Subscribed to {}", full));
                    }

                    CliCommand::Unsubscribe(topic) => {
                        if !is_valid_topic(&topic) {
                            ui_invalid_topic(&topic);
                            continue;
                        }

                        let full = format!("peerboard/v1/{}", topic);
                        let topic = IdentTopic::new(full.clone());

                        swarm.behaviour_mut().gossipsub.unsubscribe(&topic).unwrap();
                        ui_success(format!("Unsubscribed from {}", full));
                    }

                    CliCommand::Post(topic, message) => {
                        if !is_valid_topic(&topic) {
                            ui_invalid_topic(&topic);
                            continue;
                        }

                        if message.trim().is_empty() {
                            ui_error("Message cannot be empty. Use: post <topic> <message>");
                            continue;
                        }

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
                            ui_success(format!("Queued message for {}", full));
                        } else {
                            ui_error("Message was rejected by validation rules.");
                        }
                    }

                    CliCommand::View(topic) => {
                        if !is_valid_topic(&topic) {
                            ui_invalid_topic(&topic);
                            continue;
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

                        println!();
                        println!("{}", paint("Stored Messages", "1;36"));

                        let mut count = 0;
                        for row in rows {
                            if let Ok((peer, topic, content, _, nick)) = row {
                                count += 1;
                                println!(
                                    "{} {} {}",
                                    paint(&format!("[{}]", topic), "1;34"),
                                    paint(&nick, "1;37"),
                                    paint(&format!("({})", peer), "2"),
                                );
                                println!("{}\n", content);
                            }
                        }

                        if count == 0 {
                            ui_info(format!("No stored messages for {}", full_topic));
                        }
                    }

                    CliCommand::Discover => {
                        use rendezvous::Namespace;

                        let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();
                        ui_info("Looking for available Battleship peers.");
                        swarm.behaviour_mut().rendezvous.discover(Some(ns), None, None, bootstrap_peer_id);
                    }

                    CliCommand::Challenge(index) => {
                        let peers = sorted_peers(&discovered_peers);

                        if index == 0 || index > peers.len() {
                            ui_error("Invalid selection. Run 'discover' first, then use challenge <index>.");
                            continue;
                        }

                        let target = peers[index - 1];
                        selected_peer = Some(target);
                        ui_game(format!("Sending challenge to {}", short_peer(&target)));

                        swarm.behaviour_mut().challenge.send_request(
                            &target,
                            ChallengeRequest {
                                nickname: "emmanuel".to_string(),
                            },
                        );
                    }

                    CliCommand::Accept(accepted) => {
                        let Some((peer, channel)) = pending_challenge.take() else {
                            ui_warn("No pending challenge.");
                            continue;
                        };

                        let response = ChallengeResponseMsg {
                            accepted,
                        };

                        swarm
                            .behaviour_mut()
                            .challenge
                            .send_response(channel, response)
                            .unwrap();

                        if !accepted {
                            ui_info(format!("Declined challenge from {}", short_peer(&peer)));

                            selected_peer = None;
                            in_game = false;
                            is_my_turn = false;
                            shot_seq = 1;

                            // Important:
                            // If accepted = false, stop here.
                            // Do not send BoardReady.
                            // Do not unregister from rendezvous.
                            continue;
                        }

                        ui_success(format!("Accepted challenge from {}", short_peer(&peer)));
                        ui_game(format!("Game starting with {}", short_peer(&peer)));

                        selected_peer = Some(peer);
                        is_my_turn = false;
                        in_game = true;
                        toggle_logs_during_in_game(in_game, &handle);

                        ui_info("Preparing your board.");

                        let msg = pb::BattleshipRequest {
                            msg: Some(pb::battleship_request::Msg::BoardReady(pb::BoardReady {})),
                        };

                        swarm
                            .behaviour_mut()
                            .battleship
                            .send_request(&peer, BattleshipReq { msg });

                        let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();
                        swarm.behaviour_mut().rendezvous.unregister(ns, bootstrap_peer_id);
                        ui_info("Matchmaking paused while you are in this game.");
                    }

                    CliCommand::Shoot(col, row) => {
                        if !in_game {
                            ui_warn("Not in a game. Run 'discover' to find an opponent.");
                            continue;
                        }
                        if !is_my_turn {
                            ui_warn("Not your turn yet.");
                            continue;
                        }
                        if col > 9 || row > 9 {
                            ui_error("Coordinates must be 0-9.");
                            continue;
                        }
                        let col_idx = col as usize;
                        let row_idx = row as usize;

                        if my_shots[row_idx][col_idx] {
                            ui_warn("You already shot this coordinate.");
                            continue;
                        }
                        ui_game(format!("Firing at ({}, {})", col, row));
                        if let Some(target) = selected_peer {
                            my_shots[row_idx][col_idx] = true;
                            let msg = pb::BattleshipRequest {
                                msg: Some(pb::battleship_request::Msg::Shot(pb::Shot {
                                    seq: shot_seq,
                                    col,
                                    row,
                                })),
                            };

                            swarm.behaviour_mut().battleship.send_request(&target, BattleshipReq { msg });
                            shot_seq += 1;
                            is_my_turn = false;

                            print_turn(is_my_turn);
                        } else {
                            ui_error("No opponent selected. Cannot send shot.");
                        }
                    }

                    CliCommand::Resign => {
                        if !in_game {
                            ui_warn("Not in a game.");
                            continue;
                        }

                        if let Some(target) = selected_peer {
                            let msg = pb::BattleshipRequest {
                                msg: Some(pb::battleship_request::Msg::Resign(pb::Resign {})),
                            };

                            swarm.behaviour_mut().battleship.send_request(&target, BattleshipReq { msg });
                            ui_game("You resigned. Game ended.");
                            reset_game_state(
                                &mut selected_peer,
                                &mut in_game,
                                &mut is_my_turn,
                                &mut shot_seq,
                                &mut my_hits,
                                &mut my_shots,
                            );
                            toggle_logs_during_in_game(in_game, &handle);
                            let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();
                            match swarm.behaviour_mut().rendezvous.register(ns, bootstrap_peer_id, None) {
                                Ok(_) => info!("Available for matchmaking again"),
                                Err(_) => {}
                            }
                        } else {
                            ui_error("No opponent selected. Cannot resign.");
                        }
                    }

                    CliCommand::Help => {
                        print_help(in_game);
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
                                        let discovered_peer = reg.record.peer_id();
                                        if discovered_peer == peer_id {
                                            continue;
                                        }
                                        discovered_peers.insert(discovered_peer);
                                        for addr in reg.record.addresses() {
                                            swarm.behaviour_mut().kademlia.add_address(&discovered_peer, addr.clone());
                                            swarm.behaviour_mut().challenge.add_address(&discovered_peer, addr.clone());
                                            swarm.behaviour_mut().battleship.add_address(&discovered_peer, addr.clone());
                                        }
                                    }
                                    println!();
                                    println!("{}", paint("Discovered Peers", "1;36"));
                                    let peers = sorted_peers(&discovered_peers);
                                    if peers.is_empty() {
                                        ui_info("No available peers found yet. Try discover again in a moment.");
                                    } else {
                                        for (i, peer) in peers.iter().enumerate() {
                                            println!("  {:>2}. {} {}", i + 1, paint(&short_peer(peer), "1;37"), paint(&peer.to_string(), "2"));
                                        }
                                        println!("{}", paint("Use challenge <index> to invite someone.", "2"));
                                    }
                                    print_prompt();
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
                                            ui_game(format!(
                                                "Incoming challenge from {} (nickname: {})",
                                                short_peer(&peer),
                                                request.nickname
                                            ));

                                            if in_game {
                                                ui_info(format!(
                                                    "Already in a game, so the challenge from {} was declined.",
                                                    short_peer(&peer)
                                                ));

                                                let response = ChallengeResponseMsg {
                                                    accepted: false,
                                                };

                                                swarm
                                                    .behaviour_mut()
                                                    .challenge
                                                    .send_response(channel, response)
                                                    .unwrap();

                                                continue;
                                            }

                                            if pending_challenge.is_some() {
                                                ui_info(format!(
                                                    "Already have a pending challenge, so the challenge from {} was declined.",
                                                    short_peer(&peer)
                                                ));

                                                let response = ChallengeResponseMsg {
                                                    accepted: false,
                                                };
                                                swarm.behaviour_mut().challenge.send_response(channel, response).unwrap();
                                                continue;
                                            }

                                            pending_challenge = Some((peer, channel));

                                            ui_warn(format!(
                                                "Challenge pending from {}. Use: accept yes or accept no",
                                                short_peer(&peer)
                                            ));
                                        }

                                        RequestResponseMessage::Response {
                                            response,
                                            ..
                                        } => {
                                            ui_info(format!("Challenge response: accepted = {}", response.accepted));

                                            if !response.accepted {
                                                ui_warn(format!("Challenge declined by {}", short_peer(&peer)));
                                                selected_peer = None;
                                                in_game = false;
                                                is_my_turn = false;
                                                shot_seq = 1;
                                                continue;
                                            }

                                            is_my_turn = true;
                                            ui_game(format!("Game starting with {}", short_peer(&peer)));
                                            in_game = true;
                                            toggle_logs_during_in_game(in_game, &handle);

                                            if let Some(target) = selected_peer {
                                                ui_info(format!("Preparing board for {}", short_peer(&target)));
                                                let msg = pb::BattleshipRequest {
                                                    msg: Some(pb::battleship_request::Msg::BoardReady(pb::BoardReady {})),
                                                };

                                                swarm.behaviour_mut().battleship.send_request(&target, BattleshipReq { msg });
                                            }
                                            // Unregister from rendezvous
                                            use rendezvous::Namespace;
                                            let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();

                                            swarm.behaviour_mut().rendezvous.unregister(ns, bootstrap_peer_id);
                                            ui_info("Matchmaking paused while you are in this game.");
                                        }
                                    }
                                }

                            request_response::Event::OutboundFailure {
                                peer: _,
                                error: _,
                                ..
                            } => {
                                    if in_game {
                                        if let Some(target) = selected_peer {
                                            let msg = pb::BattleshipRequest {
                                                msg: Some(pb::battleship_request::Msg::Resign(pb::Resign {})),
                                            };
                                            swarm.behaviour_mut().battleship.send_request(&target, BattleshipReq { msg });
                                        }
                                        ui_warn("Ending game because the opponent did not respond.");

                                        reset_game_state(
                                            &mut selected_peer,
                                            &mut in_game,
                                            &mut is_my_turn,
                                            &mut shot_seq,
                                            &mut my_hits,
                                            &mut my_shots,
                                        );
                                        let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();

                                        if let Err(e) = swarm
                                            .behaviour_mut()
                                            .rendezvous
                                            .register(ns, bootstrap_peer_id, None)
                                        {
                                            error!("Rendezvous re-register failed after timeout: {:?}", e);
                                        } else {
                                            info!("Available for matchmaking again");
                                        }
                                        toggle_logs_during_in_game(in_game, &handle);
                                    }
                                }

                                request_response::Event::InboundFailure { peer, error, .. } => {
                                    ui_error(format!("Challenge inbound failure from {}: {:?}", short_peer(&peer), error));
                                }

                                request_response::Event::ResponseSent { peer, .. } => {
                                    ui_info(format!("Response sent to {}", short_peer(&peer)));
                                }
                            }
                        }

                        SwarmEvent::Behaviour(MyBehaviourEvent::Battleship(event)) => {
                            match event {
                                request_response::Event::Message { peer, message } => {
                                    match message {
                                        RequestResponseMessage::Request { request, channel, .. } => {
                                            match request.msg.msg {
                                                Some(pb::battleship_request::Msg::BoardReady(_)) => {
                                                    ui_info(format!("Opponent board is ready: {}", short_peer(&peer)));

                                                    let response = pb::BattleshipResponse {
                                                        msg: Some(pb::battleship_response::Msg::BoardAck(pb::BoardAck {})),
                                                    };

                                                    swarm.behaviour_mut().battleship
                                                    .send_response(channel, BattleshipRes { msg: response }).unwrap();
                                                }

                                                Some(pb::battleship_request::Msg::Shot(shot)) => {
                                                    ui_game(format!(
                                                        "Incoming shot #{} at ({}, {})",
                                                        shot.seq,
                                                        shot.col,
                                                        shot.row
                                                    ));

                                                    let mut hit = false;
                                                    let mut won = false;
                                                    let mut invalid_shot = false;

                                                    if shot.col <= 9 && shot.row <= 9 {
                                                        let col = shot.col as usize;
                                                        let row = shot.row as usize;

                                                        hit = my_board[row][col];

                                                        if hit {
                                                            my_hits[row][col] = true;
                                                        }

                                                        won = all_ship_cells_hit(&my_board, &my_hits);
                                                    } else {
                                                        invalid_shot = true;
                                                        ui_error("Opponent sent invalid coordinates.");
                                                    }

                                                    let result = pb::ShotResult {
                                                        seq: shot.seq,
                                                        hit,
                                                        sunk: false,
                                                        won,
                                                    };

                                                    let response = pb::BattleshipResponse {
                                                        msg: Some(pb::battleship_response::Msg::ShotResult(result)),
                                                    };

                                                    swarm.behaviour_mut().battleship.send_response(channel, BattleshipRes { msg: response }).unwrap();

                                                    if invalid_shot {
                                                        if let Some(target) = selected_peer {
                                                            let msg = pb::BattleshipRequest {
                                                                msg: Some(pb::battleship_request::Msg::Resign(pb::Resign {})),
                                                            };
                                                            swarm.behaviour_mut().battleship.send_request(&target, BattleshipReq { msg });
                                                        }

                                                        ui_error("Invalid shot received. Ending game with resign.");
                                                        reset_game_state(
                                                            &mut selected_peer,
                                                            &mut in_game,
                                                            &mut is_my_turn,
                                                            &mut shot_seq,
                                                            &mut my_hits,
                                                            &mut my_shots,
                                                        );
                                                        toggle_logs_during_in_game(in_game, &handle);
                                                        let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();
                                                        match swarm.behaviour_mut().rendezvous.register(ns, bootstrap_peer_id, None) {
                                                            Ok(_) => info!("Available for matchmaking again"),
                                                            Err(_) => {}
                                                        }
                                                    } else if won {
                                                        ui_game("All of your ships have been hit. You lost.");
                                                        reset_game_state(
                                                            &mut selected_peer,
                                                            &mut in_game,
                                                            &mut is_my_turn,
                                                            &mut shot_seq,
                                                            &mut my_hits,
                                                            &mut my_shots,
                                                        );
                                                        toggle_logs_during_in_game(in_game, &handle);
                                                        let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();
                                                        match swarm.behaviour_mut().rendezvous.register(ns, bootstrap_peer_id, None) {
                                                            Ok(_) => info!("Available for matchmaking again"),
                                                            Err(_) => {}
                                                        }
                                                    } else {
                                                        is_my_turn = true;
                                                        print_turn(is_my_turn);
                                                    }
                                                }

                                                Some(pb::battleship_request::Msg::Resign(_)) => {
                                                    ui_game("Opponent resigned. You win.");
                                                    let response = pb::BattleshipResponse {
                                                        msg: Some(pb::battleship_response::Msg::ResignAck(pb::ResignAck {})),
                                                    };
                                                    swarm.behaviour_mut().battleship.send_response(channel, BattleshipRes { msg: response }).unwrap();
                                                    reset_game_state(
                                                        &mut selected_peer,
                                                        &mut in_game,
                                                        &mut is_my_turn,
                                                        &mut shot_seq,
                                                        &mut my_hits,
                                                        &mut my_shots,
                                                    );
                                                    toggle_logs_during_in_game(in_game, &handle);
                                                    let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();
                                                    match swarm.behaviour_mut().rendezvous.register(ns, bootstrap_peer_id, None) {
                                                        Ok(_) => info!("Available for matchmaking again"),
                                                        Err(_) => {}
                                                    }
                                                }

                                                _ => {}
                                            }
                                        }
                                        RequestResponseMessage::Response { response, .. } => {
                                            match response.msg.msg {
                                                Some(pb::battleship_response::Msg::BoardAck(_)) => {
                                                    ui_game(format!("Boards ready with {}.", short_peer(&peer)));
                                                    print_turn(is_my_turn);
                                                }

                                                Some(pb::battleship_response::Msg::ShotResult(res)) => {
                                                    ui_game(format!(
                                                        "Shot #{} result: {}",
                                                        res.seq,
                                                        if res.hit { "hit" } else { "miss" }
                                                    ));

                                                    if res.won {
                                                        ui_game("You won!");
                                                        if let Some(target) = selected_peer {
                                                            let msg = pb::BattleshipRequest {
                                                                msg: Some(pb::battleship_request::Msg::Resign(pb::Resign {})),
                                                            };
                                                            swarm.behaviour_mut().battleship.send_request(&target, BattleshipReq { msg });
                                                        }
                                                        reset_game_state(
                                                            &mut selected_peer,
                                                            &mut in_game,
                                                            &mut is_my_turn,
                                                            &mut shot_seq,
                                                            &mut my_hits,
                                                            &mut my_shots,
                                                        );
                                                        toggle_logs_during_in_game(in_game, &handle);
                                                        let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();
                                                        match swarm.behaviour_mut().rendezvous.register(ns, bootstrap_peer_id, None) {
                                                            Ok(_) => info!("Available for matchmaking again"),
                                                            Err(_) => {}
                                                        }
                                                    } else {
                                                        is_my_turn = false;
                                                        print_turn(is_my_turn);
                                                    }
                                                }

                                                Some(pb::battleship_response::Msg::ResignAck(_)) => {
                                                    ui_success("Resign acknowledged. Game ended.");

                                                    reset_game_state(
                                                        &mut selected_peer,
                                                        &mut in_game,
                                                        &mut is_my_turn,
                                                        &mut shot_seq,
                                                        &mut my_hits,
                                                        &mut my_shots,
                                                    );
                                                    toggle_logs_during_in_game(in_game, &handle);
                                                    let ns = Namespace::new("peerboard/challenge/seeking".to_string()).unwrap();
                                                    match swarm.behaviour_mut().rendezvous.register(ns, bootstrap_peer_id, None) {
                                                        Ok(_) => info!("Available for matchmaking again"),
                                                        Err(_) => {}
                                                    }
                                                }

                                                _ => {}
                                            }
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
                    Ok(_) => ui_success("Message published."),
                    Err(_) => ui_error("Could not publish message. Try again in a moment."),
                }
            } else {
                pending_publish = Some((topic, data));
            }
        }
    }
}

fn load_or_create_identity(path: &str) -> Result<identity::Keypair, Box<dyn Error>> {
    if Path::new(path).exists() {
        ui_info(format!("Using identity file {:?}", path));
        let bytes = fs::read(path)?;
        let keypair = identity::Keypair::from_protobuf_encoding(&bytes)?;
        Ok(keypair)
    } else {
        let keypair = identity::Keypair::generate_ed25519();
        let bytes = keypair.to_protobuf_encoding()?;
        ui_success(format!("Generated new identity file {:?}", path));
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

    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM messages WHERE message_id = ?1)",
            params![msg.message_id],
            |row| row.get(0),
        )
        .unwrap_or(false);

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

fn print_welcome(peer_id: &PeerId) {
    println!();
    println!("{}", paint("PeerBoard", "1;36"));
    println!(
        "{}",
        paint("P2P message board + Battleship matchmaking", "2")
    );
    println!("Local peer: {}", paint(&short_peer(peer_id), "1;37"));
    println!(
        "Type {} whenever you want the command list.",
        paint("help", "1;33")
    );
}

fn print_help(in_game: bool) {
    println!();
    println!("{}", paint("Commands", "1;36"));

    if !in_game {
        print_command("subscribe <topic>", "Join a message board topic");
        print_command("unsubscribe <topic>", "Leave a topic");
        print_command(
            "post <topic> <message>",
            "Send a message; publish also works",
        );
        print_command(
            "view <topic>",
            "Show locally stored messages; list also works",
        );
        print_command(
            "discover",
            "Find peers who are open to Battleship; peers also works",
        );
        print_command("challenge <index>", "Challenge a discovered peer");
        print_command("accept <yes|no>", "Respond to an incoming challenge");
    } else {
        print_command("shoot <col> <row>", "Fire at a coordinate from 0 to 9");
        print_command("resign", "Concede the current game");
    }

    print_command("help", "Show this guide");
    println!(
        "{}",
        paint(
            "Topics can use lowercase letters, numbers, and hyphens.",
            "2"
        )
    );
    print_prompt();
}

fn print_command(command: &str, description: &str) {
    let padding = " ".repeat(28usize.saturating_sub(command.len()));
    println!("  {}{} {}", paint(command, "1;33"), padding, description);
}

fn print_prompt() {
    print!("\n{}", paint("[peerboard] > ", "1;36"));
    let _ = io::stdout().flush();
}

fn ui_success(message: impl AsRef<str>) {
    ui_status("OK", "1;32", message.as_ref());
}

fn ui_info(message: impl AsRef<str>) {
    ui_status("INFO", "1;34", message.as_ref());
}

fn ui_warn(message: impl AsRef<str>) {
    ui_status("WAIT", "1;33", message.as_ref());
}

fn ui_error(message: impl AsRef<str>) {
    ui_status("NOPE", "1;31", message.as_ref());
}

fn ui_invalid_topic(topic: &str) {
    ui_error(format!(
        "Invalid topic '{}'. Use lowercase letters, numbers, and hyphens.",
        topic
    ));
}

fn ui_game(message: impl AsRef<str>) {
    ui_status("GAME", "1;35", message.as_ref());
}

fn ui_status(label: &str, color: &str, message: &str) {
    println!("{} {}", paint(&format!("[{}]", label), color), message);
}

fn paint(text: &str, code: &str) -> String {
    if io::stdout().is_terminal() {
        format!("\x1b[{}m{}\x1b[0m", code, text)
    } else {
        text.to_string()
    }
}

fn trim_wrapping_quotes(s: &str) -> &str {
    let trimmed = s.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return &trimmed[1..trimmed.len() - 1];
        }
    }
    trimmed
}

fn parse_yes_no(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "yes" | "y" => Some(true),
        "false" | "no" | "n" => Some(false),
        _ => None,
    }
}

fn short_peer(peer: &PeerId) -> String {
    let id = peer.to_string();
    if id.len() <= 18 {
        id
    } else {
        format!("{}...{}", &id[..8], &id[id.len() - 6..])
    }
}

fn sorted_peers(peers: &HashSet<PeerId>) -> Vec<PeerId> {
    let mut peers: Vec<_> = peers.iter().cloned().collect();
    peers.sort_by_key(|peer| peer.to_string());
    peers
}

fn print_turn(is_my_turn: bool) {
    if is_my_turn {
        ui_game("Your turn. Use: shoot <col> <row>");
    } else {
        ui_warn("Waiting for opponent.");
    }
}

fn is_valid_topic(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

fn create_random_board() -> [[bool; 10]; 10] {
    let mut board = [[false; 10]; 10];

    for ship_len in [5usize, 4, 3, 3, 2] {
        loop {
            let horizontal = random_index(2) == 0;
            let max_row = if horizontal { 10 } else { 11 - ship_len };
            let max_col = if horizontal { 11 - ship_len } else { 10 };
            let row = random_index(max_row);
            let col = random_index(max_col);

            let clear = (0..ship_len).all(|offset| {
                let row = if horizontal { row } else { row + offset };
                let col = if horizontal { col + offset } else { col };
                !board[row][col]
            });

            if clear {
                for offset in 0..ship_len {
                    let row = if horizontal { row } else { row + offset };
                    let col = if horizontal { col + offset } else { col };
                    board[row][col] = true;
                }

                break;
            }
        }
    }

    board
}

fn random_index(max: usize) -> usize {
    let bytes = *Uuid::new_v4().as_bytes();
    let value = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    (value as usize) % max
}

fn all_ship_cells_hit(board: &[[bool; 10]; 10], hits: &[[bool; 10]; 10]) -> bool {
    for row in 0..10 {
        for col in 0..10 {
            if board[row][col] && !hits[row][col] {
                return false;
            }
        }
    }

    true
}

fn reset_game_state(
    selected_peer: &mut Option<PeerId>,
    in_game: &mut bool,
    is_my_turn: &mut bool,
    shot_seq: &mut u32,
    my_hits: &mut [[bool; 10]; 10],
    my_shots: &mut [[bool; 10]; 10],
) {
    *selected_peer = None;
    *in_game = false;
    *is_my_turn = false;
    *shot_seq = 1;
    *my_hits = [[false; 10]; 10];
    *my_shots = [[false; 10]; 10];
}

fn toggle_logs_during_in_game<S>(b: bool, handle: &reload::Handle<EnvFilter, S>) {
    let level = if b { "warn" } else { "info" };
    handle.reload(EnvFilter::new(level)).unwrap();
}
