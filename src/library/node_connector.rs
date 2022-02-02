use std::fmt::{Display, Formatter};
use redis::{ErrorKind, FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs, Value};
use crate::domain::PathRequest;

type BasicResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug)]
pub(crate) enum ConnectionError {
    DeserializationError(zeromq::ZmqMessage),
    TargetDoesNotExist(usize),
    ProtocolError(zeromq::ZmqError),
    NoRequest,
    RedisDeserializationError(RedisError)
}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        return match self {
            ConnectionError::DeserializationError(msg) => { write!(f, "Cannot deserialize message {:?} to string", msg) }
            ConnectionError::TargetDoesNotExist(target_id) => { write!(f, "Cannot send message to non existing server with id {:?}", target_id) }
            ConnectionError::ProtocolError(err) => { err.fmt(f) }
            ConnectionError::NoRequest => { write!(f, "No request received!") }
            ConnectionError::RedisDeserializationError(err) => { err.fmt(f) }
        };
    }
}

impl std::error::Error for ConnectionError {}


impl ToRedisArgs for PathRequest {
    fn write_redis_args<W>(&self, out: &mut W) where W: ?Sized + RedisWrite {
        let json_string = serde_json::to_string(self).unwrap();
        String::write_redis_args(&json_string, out);
    }
}

impl FromRedisValue for PathRequest {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let json_string = String::from_redis_value(v)?;
        match serde_json::from_str(&json_string) {
            Ok(x) => Ok(x),
            Err(e) => { Err(RedisError::from((ErrorKind::TypeError, "Failed to deserialize json: ", e.to_string()))) }
        }
    }
}

#[async_trait::async_trait]
pub(crate) trait NodeListener: Sync {
    async fn get_new_request(&mut self) -> Result<PathRequest, ConnectionError>;
}


#[async_trait::async_trait]
pub(crate) trait ResultReplier: Send + Sync + ResultReplierClone {
    async fn send(&self, reply: &PathRequest) -> BasicResult<()>;
}

pub(crate) trait ResultReplierClone {
    fn clone_box(&self) -> Box<dyn ResultReplier>;
}

impl<T: 'static + ResultReplier + Clone> ResultReplierClone for T {
    fn clone_box(&self) -> Box<dyn ResultReplier> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn ResultReplier> {
    fn clone(&self) -> Box<dyn ResultReplier> {
        self.clone_box()
    }
}

#[async_trait::async_trait]
pub(crate) trait NodeSender: Send + Sync + NodeSenderClone {
    async fn send_request(&self, target_id: usize, request: PathRequest) -> BasicResult<()>;
}

pub(crate) trait NodeSenderClone {
    fn clone_box(&self) -> Box<dyn NodeSender>;
}

impl<T: 'static + NodeSender + Clone> NodeSenderClone for T {
    fn clone_box(&self) -> Box<dyn NodeSender> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn NodeSender> {
    fn clone(&self) -> Box<dyn NodeSender> {
        self.clone_box()
    }
}

pub(crate) mod zmq_connector {
    use std::collections::BTreeMap;
    use std::fmt::{Display, Formatter};
    use std::sync::Arc;
    use zeromq::{Socket, SocketRecv, SocketSend, ZmqMessage};
    use crate::node_connector::BasicResult;
    use crate::{ConnectionError, NodeListener, NodeSender, ResultReplier};
    use crate::domain::PathRequest;
    use crate::redis_connector::NetworkInfo;

    pub(crate) struct ZMQNodeListener {
        listen_sck: zeromq::PullSocket,
    }

    impl ZMQNodeListener {
        pub(crate) async fn new(addr: &str) -> BasicResult<Self> {
            let mut listen_sck = zeromq::PullSocket::new();
            listen_sck.bind(addr).await?;
            Ok(ZMQNodeListener {
                listen_sck
            })
        }
    }

    #[async_trait::async_trait]
    impl NodeListener for ZMQNodeListener {
        async fn get_new_request(&mut self) -> Result<PathRequest, ConnectionError> {
            let zmq_msg: ZmqMessage = self.listen_sck.recv().await.map_err(|e| ConnectionError::ProtocolError(e))?;
            let msg_str = String::from_utf8(zmq_msg.get(0).unwrap().to_vec()).map_err(|_| ConnectionError::DeserializationError(zmq_msg.clone()))?;
            serde_json::from_str::<PathRequest>(&msg_str).map_err(|_| ConnectionError::DeserializationError(zmq_msg))
        }
    }

    #[derive(Clone)]
    pub(crate) struct ZMQReplier {
        socket: Arc<tokio::sync::Mutex<zeromq::PushSocket>>,
        url: String,
    }

    impl Display for ZMQReplier {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.url)
        }
    }

    impl ZMQReplier {
        pub(crate) async fn new(url: &str) -> BasicResult<Self> {
            let mut socket = zeromq::PushSocket::new();
            socket.connect(url).await?;
            Ok(ZMQReplier {
                socket: Arc::new(tokio::sync::Mutex::new(socket)),
                url: String::from(url),
            })
        }
    }

    #[async_trait::async_trait]
    impl ResultReplier for ZMQReplier {
        async fn send(&self, reply: &PathRequest) -> BasicResult<()> {
            let mut target_sck_guard = self.socket.lock().await;
            let raw_request = serde_json::to_vec(&reply)?;
            Ok(target_sck_guard.send(raw_request.into()).await?)
        }
    }

    #[derive(Clone)]
    pub struct ZMQConnectionsManager {
        node_connections: Arc<BTreeMap<usize, tokio::sync::Mutex<zeromq::ReqSocket>>>,
        network_info: NetworkInfo,
    }

    impl ZMQConnectionsManager {
        pub(crate) async fn new(network_info: NetworkInfo) -> BasicResult<Self> {
            let mut node_connections = BTreeMap::new();
            for (id, server_info) in network_info.get_servers().await {
                let mut request_sck = zeromq::ReqSocket::new();
                request_sck.connect(&server_info.addr).await?;
                node_connections.insert(id, tokio::sync::Mutex::new(request_sck));
            }
            Ok(ZMQConnectionsManager {
                node_connections: Arc::new(node_connections),
                network_info,
            })
        }
    }

    #[async_trait::async_trait]
    impl NodeSender for ZMQConnectionsManager {
        async fn send_request(&self, target_id: usize, request: PathRequest) -> BasicResult<()> { // todo dont send to self
            loop {
                let mut target_sck_guard = self.node_connections.get(&target_id).ok_or(ConnectionError::TargetDoesNotExist(target_id))?.lock().await;
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
}

pub(crate) mod redis_connector {
    use std::fmt::{Display, Formatter};
    use std::pin::Pin;
    use futures_util::StreamExt;
    use redis::{AsyncCommands, Msg};
    use crate::node_connector::{BasicResult};
    use crate::{ConnectionError, NodeListener, NodeSender, RedisConnector, ResultReplier};
    use crate::domain::PathRequest;


    pub(crate) struct RedisNodeListener {
        stream: Pin<Box<dyn futures_util::Stream<Item=Msg> + Sync + Send>>,
    }

    impl RedisNodeListener {
        pub(crate) async fn new(redis_connector: &RedisConnector, id: usize) -> BasicResult<Self> {
            let connection = redis_connector.spawn_connection().await?;
            let mut pubsub = connection.into_pubsub();
            pubsub.subscribe(format!("node_{}", id)).await?;
            let stream = Box::pin(pubsub.into_on_message());
            Ok(Self {
                stream,
            })
        }
    }

    #[async_trait::async_trait]
    impl NodeListener for RedisNodeListener {
        async fn get_new_request(&mut self) -> Result<PathRequest, ConnectionError> {
            self.stream.next().await.ok_or(ConnectionError::NoRequest)?.get_payload().map_err(|err| ConnectionError::RedisDeserializationError(err))
        }
    }

    #[derive(Clone)]
    pub(crate) struct RedisReplier {
        redis_connector: RedisConnector
    }

    impl Display for RedisReplier {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "RedisReplier")
        }
    }

    impl RedisReplier {
        pub(crate) async fn new(redis_connector: RedisConnector) -> BasicResult<Self> {
            Ok(Self {
                redis_connector,
            })
        }
    }

    #[async_trait::async_trait]
    impl ResultReplier for RedisReplier {
        async fn send(&self, reply: &PathRequest) -> BasicResult<()> {
            let (_count_guard, mut conn) = self.redis_connector.claim_connection().await;
            let res = conn.publish(format!("results_{}", reply.request_id), reply).await;
            self.redis_connector.release_connection(conn).await;
            res?;
            Ok(())
        }
    }

    #[derive(Clone)]
    pub struct RedisConnectionsManager {
        redis_connector: RedisConnector,
    }

    impl RedisConnectionsManager {
        pub(crate) async fn new(redis_connector: RedisConnector) -> BasicResult<Self> {
            Ok(Self {
                redis_connector,
            })
        }
    }

    #[async_trait::async_trait]
    impl NodeSender for RedisConnectionsManager {
        async fn send_request(&self, target_id: usize, request: PathRequest) -> BasicResult<()> { // todo dont send to self
            let (_count_guard, mut conn) = self.redis_connector.claim_connection().await;
            let res = conn.publish(format!("node_{}", target_id), request).await;
            self.redis_connector.release_connection(conn).await;
            res?;
            Ok(())
        }
    }
}
