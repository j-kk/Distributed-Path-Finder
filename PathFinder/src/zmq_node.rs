extern crate zmq;

use std::{error, fmt};
use std::collections::BTreeMap;
use std::fmt::Formatter;
use serde::{Deserialize, Serialize};
use zmq::{Socket};
use crate::pathfinder::{VertexInfo};
use crate::redis_connector::NetworkInfo;

type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

#[derive(Debug, Clone)]
enum ZMQError {
    DeserializationError(Vec<u8>),
    TargetDoesNotExist(usize)
}

impl fmt::Display for ZMQError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ZMQError::DeserializationError(msg) => { write!(f, "Cannot deserialize message {:?} to string", msg) }
            ZMQError::TargetDoesNotExist(target_id) => { write!(f, "Cannot send message to non existing server with id {:?}", target_id) }
        }
    }
}

impl error::Error for ZMQError {}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct PathRequest {
    source: VertexInfo,
    target: VertexInfo,
    path: Vec<u64>,
    cost: u64,
}

// TODO next
// create map of nodes by starting server info and then update on client ask
pub struct NodeListener {
    listen_sck: Socket,
}

impl NodeListener {
    fn new(zmq_context: &zmq::Context, addr: &str) -> Result<Self> {
        let listen_sck = zmq_context.socket(zmq::REP)?;
        listen_sck.bind(addr)?;
        Ok(NodeListener {
            listen_sck
        })
    }

    fn get_new_request(&self) -> Result<PathRequest> {
        let mut zmq_msg = zmq::Message::new();
        self.listen_sck.recv(&mut zmq_msg, 0)?;
        let msg_str = zmq_msg.as_str().ok_or(ZMQError::DeserializationError(zmq_msg.to_vec()))?;

        let msg = serde_json::from_str::<PathRequest>(&msg_str);
        self.listen_sck.send("OK", 0)?;
        return Ok(msg?)
    }
}


struct NodeConnectionsManager {
    node_connections: BTreeMap<usize, tokio::sync::Mutex<Socket>>,
    network_info: NetworkInfo,
}

impl NodeConnectionsManager {
    async fn new(context: &zmq::Context, network_info: NetworkInfo) -> Result<NodeConnectionsManager> {
        let mut node_connections: BTreeMap<usize, tokio::sync::Mutex<Socket>> = BTreeMap::new();
        for (id, server_info) in network_info.get_servers().await {
            let request_sck = context.socket(zmq::REQ)?;
            request_sck.connect(&server_info.addr)?;
            node_connections.insert(id, tokio::sync::Mutex::new(request_sck));
        }
        Ok(NodeConnectionsManager {
            node_connections,
            network_info,
        })
    }

    async fn send_request(&self, target_id: usize, request: PathRequest) -> Result<()> { // todo dont send to self
        loop {
            let target_sck_guard = self.node_connections.get(&target_id).ok_or(ZMQError::TargetDoesNotExist(target_id))?.lock().await;
            let raw_request = serde_json::to_vec(&request)?;
            target_sck_guard.send(&raw_request, 0)?;
            let mut zmq_msg = zmq::Message::new();
            target_sck_guard.recv(&mut zmq_msg, 0)?;
            if let Some(response) = zmq_msg.as_str() {
                if response == "OK" {
                    return Ok(());
                } else {
                    log::warn!("Node {} responded with message: {}", target_id, response);
                }
            } else {
                log::warn!("Node {} responded with illegible message: {:?}", target_id, zmq_msg.as_ref());
            }
        }
    }
}
