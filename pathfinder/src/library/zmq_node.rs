use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use zeromq::{Socket, SocketRecv, SocketSend, ZmqMessage};
use crate::graph::{NodeIdx, RegionIdx};
use crate::redis_connector::NetworkInfo;

type BasicResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug)]
pub(crate) enum ZMQError {
    DeserializationError(zeromq::ZmqMessage),
    TargetDoesNotExist(usize),
    ProtocolError(zeromq::ZmqError)
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

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct NodeInfo (pub(crate) NodeIdx, pub(crate) RegionIdx);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct PathRequest {
    source: NodeInfo,
    pub(crate) target: NodeInfo,
    path: Vec<NodeIdx>,
    pub(crate) last_node: NodeInfo,
    cost: u64,
}

impl PathRequest {
    pub(crate) fn new(source: NodeInfo,
                      target: NodeInfo,
                      path: Vec<NodeIdx>,
                      last_node: NodeInfo,
                      cost: u64) -> PathRequest {
        PathRequest {
            source,
            target,
            path,
            last_node,
            cost,
        }
    }

    pub(crate) fn update(&self,
                         mut path: Vec<NodeIdx>,
                         last_node: NodeInfo,
                         cost: u64) -> Self {
        let mut new_path = self.path.clone();
        new_path.append(&mut path);
        PathRequest::new(
            self.source.clone(),
            self.target.clone(),
            new_path,
            last_node,
            self.cost.clone() + cost
        )
    }
}

pub struct NodeListener {
    listen_sck: zeromq::PullSocket
}

impl NodeListener {
    pub(crate) async fn new(addr: &str) -> BasicResult<Self> {
        let mut listen_sck = zeromq::PullSocket::new();
        listen_sck.bind(addr).await?;
        Ok(NodeListener {
            listen_sck
        })
    }

    pub(crate) async fn get_new_request(&mut self) -> Result<PathRequest, ZMQError> {
        let zmq_msg: ZmqMessage = self.listen_sck.recv().await.map_err(|e| ZMQError::ProtocolError(e))?;
        let msg_str = String::from_utf8(zmq_msg.get(0).unwrap().to_vec()).map_err(|_| ZMQError::DeserializationError(zmq_msg.clone()))?;
        serde_json::from_str::<PathRequest>(&msg_str).map_err(|_| ZMQError::DeserializationError(zmq_msg))
    }
}

#[derive(Clone)]
pub(crate) struct ReplyConnector {
    socket: Arc<tokio::sync::Mutex<zeromq::PushSocket>>,
    url: String
}

impl Display for ReplyConnector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.url)
    }
}

impl ReplyConnector {
    pub(crate) async fn new(url: &str) -> BasicResult<ReplyConnector> {
        let mut socket = zeromq::PushSocket::new();
        socket.connect(url).await?;
        Ok(ReplyConnector {
            socket: Arc::new(tokio::sync::Mutex::new(socket)),
            url: String::from(url)
        })
    }

    pub(crate) async fn send(&self, reply: &PathRequest) -> BasicResult<()> {
        let mut target_sck_guard = self.socket.lock().await;
        let raw_request = serde_json::to_vec(&reply)?;
        Ok(target_sck_guard.send(raw_request.into()).await?)
    }
}

#[derive(Clone)]
pub struct NodeConnectionsManager {
    node_connections: Arc<BTreeMap<usize, tokio::sync::Mutex<zeromq::ReqSocket>>>,
    network_info: NetworkInfo,
}

impl NodeConnectionsManager {
    pub(crate) async fn new(network_info: NetworkInfo) -> BasicResult<NodeConnectionsManager> {
        let mut node_connections= BTreeMap::new();
        for (id, server_info) in network_info.get_servers().await {
            let mut request_sck = zeromq::ReqSocket::new();
            request_sck.connect(&server_info.addr).await?;
            node_connections.insert(id, tokio::sync::Mutex::new(request_sck));
        }
        Ok(NodeConnectionsManager {
            node_connections: Arc::new(node_connections),
            network_info,
        })
    }

    pub(crate) async fn send_request(&self, target_id: usize, request: PathRequest) -> BasicResult<()> { // todo dont send to self
        loop {
            let mut target_sck_guard = self.node_connections.get(&target_id).ok_or(ZMQError::TargetDoesNotExist(target_id))?.lock().await;
            let raw_request = serde_json::to_vec(&request)?;
            target_sck_guard.send(raw_request.into()).await?;
            let zmq_msg = target_sck_guard.recv().await?;
            if let Ok(response) = String::from_utf8(zmq_msg.get(0).unwrap().to_vec()) {
                if response == "OK" {
                    return Ok(());
                } else {
                    log::warn!("Node {} responded with message: {}", target_id, response);
                }
            } else {
                log::warn!("Node {} responded with illegible message: {:?}", target_id, zmq_msg);
            }
        }
    }
}
