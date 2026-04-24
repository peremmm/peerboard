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
    StreamProtocol,
    SwarmBuilder,
};
use std::{
    error::Error,
    fs,
    path::Path,
    time::Duration,
};
use futures::StreamExt;
use tracing::{info, error};
use tracing_subscriber;

const IDENTITY_FILE: &str = "identity.key";

// Hardcoded bootstrap node
const BOOTSTRAP_ADDR: &str = "/ip4/170.64.177.57/tcp/8000/p2p/12D3KooWCvwqT3JUzVQczCvAVFa9EGzNqjHHSMVHVhm3RVyscCNY";

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    identify: identify::Behaviour,
    kademlia: KademliaBehaviour<MemoryStore>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let mut bootstrap_done = false;

    // logging purposes
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let keypair = load_or_create_identity()?;
    let peer_id = PeerId::from(keypair.public());

    info!("Peer ID: {}", peer_id);

    let mut swarm = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
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

            Ok(MyBehaviour {
                identify: identify::Behaviour::new(
                    identify::Config::new(
                        "/peerboard/identify/1.0.0".into(),
                        key.public(),
                    )
                ),
                kademlia,
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


    // event loop magic
    loop {

        match swarm.select_next_some().await {

            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on {}", address);
            }

            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                info!("Connected to {}", peer_id);

                if peer_id == bootstrap_peer_id && bootstrap_done {
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
                                match result {
                                    Ok(_) => info!("Kademlia bootstrap completed"),
                                    Err(e) => error!("Bootstrap error: {:?}", e),
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

            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                error!("Failed to connect to {:?}: {:?}", peer_id, error);
            }

            _ => {}
        }

    }
}

// local file as db
fn load_or_create_identity() -> Result<identity::Keypair, Box<dyn Error>> {
    if Path::new(IDENTITY_FILE).exists() {
        println!("Existing identity file found at {:?}", IDENTITY_FILE);
        let bytes = fs::read(IDENTITY_FILE)?;
        let keypair = identity::Keypair::from_protobuf_encoding(&bytes)?;
        Ok(keypair)
    } else {
        let keypair = identity::Keypair::generate_ed25519();
        let bytes = keypair.to_protobuf_encoding()?;
        println!("New identity file generated at {:?}", IDENTITY_FILE);
        fs::write(IDENTITY_FILE, bytes)?;
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