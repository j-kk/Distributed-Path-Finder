use std::collections::BTreeMap;
use std::sync::Arc;
use futures_util::StreamExt as _;
use redis::{AsyncCommands, FromRedisValue, RedisResult, Value, ErrorKind, RedisError, ToRedisArgs, RedisWrite};
use redis::aio::{Connection};
use serde::{Serialize, Deserialize};
use tokio::sync::SemaphorePermit;
use tokio::task::JoinHandle;
use crate::Graph;
use crate::graph::{NodeIdx, RegionIdx};


macro_rules! invalid_type_error {
    ($v:expr, $det:expr) => {{
        return Err(::std::convert::From::from(
            RedisError::from((
                ErrorKind::TypeError,
                "Response was of incompatible type",
                format!("{:?} (response was {:?})", $det, $v),
            ))));
    }};
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct ServerInfo {
    id: usize,
    pub(crate) addr: Box<str>,
    regions: Vec<RegionIdx>,
}

impl ServerInfo {
    pub fn new(id: usize,
               addr: Box<str>,
               regions: Vec<RegionIdx>) -> Self {
        Self {
            id,
            addr,
            regions,
        }
    }
}

impl ToRedisArgs for ServerInfo {
    fn write_redis_args<W>(&self, out: &mut W) where W: ?Sized + RedisWrite {
        let json_string = serde_json::to_string(self).unwrap();
        String::write_redis_args(&json_string, out);
    }
}

impl FromRedisValue for ServerInfo {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let json_string = String::from_redis_value(v)?;
        match serde_json::from_str(&json_string) {
            Ok(x) => Ok(x),
            Err(e) => { Err(RedisError::from((ErrorKind::TypeError, "Failed to deserialize json: ", e.to_string()))) }
        }
    }
}


#[derive(Debug, Clone)]
struct BulkServerInfo {
    servers: BTreeMap<usize, ServerInfo>,
}

impl FromRedisValue for BulkServerInfo {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Bulk(ref items) => {
                if items.len() % 2 == 1 {
                    invalid_type_error!(v, "Response type has odd number of fields.")
                } else {
                    let mut servers = BTreeMap::new();
                    for pair in items.chunks(2).into_iter() {
                        let server_id = usize::from_redis_value(&pair[0])?;
                        let server_info = ServerInfo::from_redis_value(&pair[1])?;
                        servers.insert(server_id, server_info);
                    }
                    Ok(Self {
                        servers
                    })
                }
            }
            _ => {
                invalid_type_error!(v, "Response type is expected to be a result of hgetall.")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct NetworkInfo {
    servers: Arc<tokio::sync::RwLock<BTreeMap<usize, ServerInfo>>>,
}

impl NetworkInfo {
    fn new(servers: Arc<tokio::sync::RwLock<BTreeMap<usize, ServerInfo>>>) -> Self {
        NetworkInfo {
            servers
        }
    }

    pub(crate) async fn get_servers(&self) -> BTreeMap<usize, ServerInfo> {
        let servers_reader = self.servers.read().await;
        servers_reader.clone()
    }

    pub(crate) async fn get_server(&self, id: usize) -> Option<ServerInfo> {
        let servers_reader = self.servers.read().await;
        servers_reader.get(&id).map(|x| x.clone())
    }
}

pub(crate) struct NetworkManager {
    pub(crate) network_info: NetworkInfo,
    update_task: JoinHandle<()>,
}

impl NetworkManager {
    async fn new(hget_conn: &mut redis::aio::Connection,
                 pubsub_conn: redis::aio::Connection) -> RedisResult<Self> {
        let mut pubsub = pubsub_conn.into_pubsub();
        pubsub.subscribe("server_updates").await?;

        let res: BulkServerInfo = hget_conn.hgetall("server_info").await?;

        let servers = Arc::new(tokio::sync::RwLock::new(res.servers));
        let servers_for_task = servers.clone();
        let update_task = tokio::task::spawn(async move {
            let mut pubsub_stream = pubsub.on_message();
            loop {
                let server_update: ServerInfo = pubsub_stream.next().await.unwrap().get_payload().unwrap(); // todo unsafe unwrap
                let mut servers_guard = servers_for_task.write().await;
                servers_guard.insert(server_update.id, server_update);
            }
        });

        Ok(NetworkManager {
            network_info: NetworkInfo::new(servers),
            update_task,
        })
    }
}


#[derive(Clone)]
pub struct RedisConnector {
    client: redis::Client,
    conn_pool: Arc<tokio::sync::Mutex<Vec<redis::aio::Connection>>>,
    conn_count: Arc<tokio::sync::Semaphore>,
}

impl RedisConnector {
    pub(crate) async fn new(redis_url: &str,
                            connection_count: usize) -> RedisResult<Self> {
        let client = redis::Client::open(redis_url)?;
        let mut conn_pool = Vec::new();
        for _ in 0..connection_count {
            conn_pool.push(client.get_async_connection().await?);
        }
        Ok(RedisConnector {
            client,
            conn_pool: Arc::new(tokio::sync::Mutex::new(conn_pool)),
            conn_count: Arc::new(tokio::sync::Semaphore::new(connection_count)),
        })
    }

    pub(crate) async fn claim_connection(&self) -> (SemaphorePermit<'_>, redis::aio::Connection) {
        let permit = self.conn_count.acquire().await.unwrap(); // todo unwrap
        let conn = {
            let mut pool_guard = self.conn_pool.lock().await;
            pool_guard.pop().unwrap()
        };
        return (permit, conn);
    }

    pub(crate) async fn release_connection(&self, conn: Connection) { // todo may be replaced with drop trait on connection
        let mut pool_guard = self.conn_pool.lock().await;
        pool_guard.push(conn)
    }

    pub(crate) async fn get_server_id(&self, region_id: RegionIdx) -> RedisResult<usize> {
        let (_count_guard, mut conn) = self.claim_connection().await;
        let res = conn.get(format!("region_server_{}", region_id)).await;
        self.release_connection(conn).await;
        res
    }

    pub(crate) async fn get_servers_info(&self) -> RedisResult<NetworkManager> {
        let pubsub_conn = self.client.get_async_connection().await?;
        let (_count_guard, mut conn) = self.claim_connection().await;
        let res = NetworkManager::new(&mut conn, pubsub_conn).await;
        self.release_connection(conn).await;
        res
    }

    async fn register_server(&self, server_info: &ServerInfo) -> RedisResult<()> {
        let (_count_guard, mut conn) = self.claim_connection().await;
        let r1 = conn.publish("server_updates", server_info).await;
        let r2 = conn.hset("server_info", server_info.id, server_info).await;
        self.release_connection(conn).await;
        r1?;
        r2?;
        Ok(())
    }

    pub(crate) async fn get_region(&self, node_id: NodeIdx) -> RedisResult<RegionIdx> {
        let (_count_guard, mut conn) = self.claim_connection().await;
        let region = conn.get(format!("node_region_{}", node_id)).await;
        self.release_connection(conn).await;
        region
    }

    pub(crate) async fn spawn_connection(&self) -> RedisResult<redis::aio::Connection> {
        self.client.get_async_connection().await
    }

    pub(crate) async fn set_group(&self, region_id: RegionIdx, group_id: usize) -> RedisResult <()> {
        let (_count_guard, mut conn) = self.claim_connection().await;
        let res = conn.set(format!("region_server_{}", region_id), group_id).await;
        self.release_connection(conn).await;
        res
    }

    pub(crate) async fn set_region(&self, graph: &Graph, region_id: RegionIdx) -> RedisResult<()> {
        let (_count_guard, mut conn) = self.claim_connection().await;
        let mut nodes_ids = vec![];
        let mut nodes_vals = vec![];
        for (id, node) in graph.nodes.iter() {
            if node.region == region_id {
                nodes_vals.push((format!("node_region_{}", id), region_id));
                nodes_ids.push(format!("node_region_{}", id));
            }
        }
        let res1 = conn.del(&*nodes_ids).await;
        let res2 = conn.mset_nx(&*nodes_vals).await;
        self.release_connection(conn).await;
        res1?;
        res2

    }
}
