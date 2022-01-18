use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use zmq::{Socket};
use crate::graph::{NodeIdx, RegionIdx};
use crate::redis_connector::NetworkInfo;

type BasicResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Clone)]
pub(crate) enum ZMQError {
    DeserializationError(Vec<u8>),
    TargetDoesNotExist(usize),
    ProtocolError(zmq::Error)
}

impl Display for ZMQError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        return match self {
            ZMQError::DeserializationError(msg) => { write!(f, "Cannot deserialize message {:?} to string", msg) }
            ZMQError::TargetDoesNotExist(target_id) => { write!(f, "Cannot send message to non existing server with id {:?}", target_id) }
            ZMQError::ProtocolError(err) => { err.fmt(f) }
        }
    }
}

impl std::error::Error for ZMQError {}

pub type NodeInfo = (NodeIdx, RegionIdx);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct PathRequest {
    source: NodeInfo,
    pub(crate) target: NodeInfo,
    path: Vec<NodeIdx>,
    cost: u64,
}

impl PathRequest {
    pub(crate) fn new(source: NodeInfo,
                      target: NodeInfo,
                      path: Vec<NodeIdx>,
                      cost: u64) -> PathRequest {
        PathRequest {
            source,
            target,
            path,
            cost,
        }
    }

    pub(crate) fn update(&self,
                         mut path: Vec<NodeIdx>,
                         cost: u64) -> Self {
        let mut new_path = self.path.clone();
        new_path.append(&mut path);
        PathRequest::new(
            self.source.clone(),
            self.target.clone(),
            new_path,
            self.cost.clone() + cost
        )
    }

    pub(crate) fn get_last_node(&self) -> NodeIdx {
        *self.path.last().unwrap_or(&self.source.0)
    }
}

pub struct NodeListener {
    listen_sck: Socket
}

impl NodeListener {
    fn new(zmq_context: &zmq::Context, addr: &str) -> BasicResult<Self> {
        let listen_sck = zmq_context.socket(zmq::PULL)?;
        listen_sck.bind(addr)?;
        Ok(NodeListener {
            listen_sck
        })
    }

    pub(crate) fn get_new_request(&self) -> Result<PathRequest, ZMQError> {
        let mut zmq_msg = zmq::Message::new();
        self.listen_sck.recv(&mut zmq_msg, 0).map_err(|e| ZMQError::ProtocolError(e))?;
        let msg_str = zmq_msg.as_str().ok_or(ZMQError::DeserializationError(zmq_msg.to_vec()))?;
        serde_json::from_str::<PathRequest>(&msg_str).map_err(|e| ZMQError::DeserializationError(zmq_msg.to_vec()))
    }
}

pub(crate) struct ReplyConnector {
    socket: Arc<tokio::sync::Mutex<Socket>>,
    url: String
}

impl Display for ReplyConnector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.url)
    }
}

impl ReplyConnector {
    fn new(context: &zmq::Context, url: &str) -> BasicResult<ReplyConnector> {
        let socket = context.socket(zmq::PUSH)?;
        socket.connect(url)?;
        Ok(ReplyConnector {
            socket: Arc::new(tokio::sync::Mutex::new(socket)),
            url: String::from(url)
        })
    }

    pub(crate) async fn send(&self, reply: &PathRequest) -> BasicResult<()> {
        let target_sck_guard = self.socket.lock().await;
        let raw_request = serde_json::to_vec(&reply)?;
        Ok(target_sck_guard.send(&raw_request, 0)?)
    }
}

pub struct NodeConnectionsManager {
    node_connections: BTreeMap<usize, tokio::sync::Mutex<Socket>>,
    network_info: NetworkInfo,
}

impl NodeConnectionsManager {
    async fn new(context: &zmq::Context, network_info: NetworkInfo) -> BasicResult<NodeConnectionsManager> {
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

    pub(crate) async fn send_request(&self, target_id: usize, request: PathRequest) -> BasicResult<()> { // todo dont send to self
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
                log::warn!("Node {} responded with illegible message: {:?}", target_id, zmq_msg.to_vec());
            }
        }
    }
}
