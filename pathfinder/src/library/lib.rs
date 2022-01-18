use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use async_channel::{Receiver, Sender, unbounded};
use tokio::task::JoinHandle;
use crate::graph::{Graph, GraphError, PathResult, RegionIdx};
use crate::graph_provider::{GraphProvider, GroupInfoProvider};
use crate::redis_connector::RedisConnector;
use crate::zmq_node::{NodeConnectionsManager, NodeInfo, NodeListener, PathRequest, ReplyConnector, ZMQError};

mod zmq_node;
mod graph;
mod redis_connector;
mod graph_provider;
mod error;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Clone)]
pub struct Configuration {
    google_region: String,
    google_bucket: String,
    google_access_key: String,
    google_secret_key: String,
    id: usize,
    redis_url: String,
    redis_connection_count: usize,
    listen_addr: String,
    reply_addr: String,
    worker_count: usize,
}

impl Configuration {
    pub fn from_env() -> Result<Configuration>{
        Ok(Configuration {
            google_region: env::var("GOOGLE_CLOUD_REGION")?,
            google_bucket: env::var("GOOGLE_CLOUD_BUCKET")?,
            google_access_key: env::var("GOOGLE_ACCESS_KEY")?,
            google_secret_key: env::var("GOOGLE_SECRET_KEY")?,
            id: env::var("GROUP_ID")?.parse()?,
            redis_url: env::var("REDIS_URL")?,
            redis_connection_count: env::var("REDIS_CONNECTION_COUNT")?.parse()?,
            listen_addr: env::var("LISTEN_ADDR")?,
            reply_addr: env::var("REPLY_ADDR")?,
            worker_count: env::var("WORKER_COUNT")?.parse()?,
        })
    }
}


pub struct Server {
    zmq_listener: NodeListener,
    workers: Vec<JoinHandle<()>>,
    task_senders: Vec<Sender<PathRequest>>,
    free_receiver: Receiver<usize>,
}

struct Worker {
    redis_connector: RedisConnector,
    graphs: Arc<HashMap<RegionIdx, Graph>>,
    zmq_reply: ReplyConnector,
    zmq_conn_mgr: NodeConnectionsManager,
    task_receiver: Receiver<PathRequest>,
    free_sender: Sender<usize>,
    id: usize,
}

impl Worker {
    async fn new(redis_connector: RedisConnector,
                 graphs: Arc<HashMap<RegionIdx, Graph>>,
                 zmq_reply: ReplyConnector,
                 zmq_conn_mgr: NodeConnectionsManager,
                 task_receiver: Receiver<PathRequest>,
                 free_sender: Sender<usize>,
                 id: usize) -> Result<Worker> {
        free_sender.send(id).await?;
        Ok(Worker {
            redis_connector,
            graphs,
            zmq_reply,
            zmq_conn_mgr,
            task_receiver,
            free_sender,
            id,
        })
    }

    async fn serve_request(&self, request: &PathRequest) -> Result<()> {
        let start_node = request.last_node;
        let path_result = self.graphs.get(&start_node.1).ok_or(GraphError::NodeNotFound(start_node.0, start_node.1))?.find_way(start_node, request.target)?;
        match path_result {
            PathResult::TargetReached(path, cost) => {
                let reply = request.update(path, request.target.clone(),cost);
                self.zmq_reply.send(&reply).await?;
            }
            PathResult::Continue(path, cost, next_idx) => {
                let next_region = self.redis_connector.get_region(next_idx).await?;
                let next_node = NodeInfo(next_idx, next_region);
                let new_request = request.update(path, next_node, cost);
                let server_id = self.redis_connector.get_server_id(next_region).await?;
                self.zmq_conn_mgr.send_request(server_id, new_request).await?;
            }
        }
        Ok(())
    }

    async fn work(&self) {
        loop {
            match self.task_receiver.recv().await {
                Ok(request) => {
                    if let Err(err) = self.serve_request(&request).await {
                        log::info!("Worker {} couldn't handle request {:?}, details: {:?}", self.id, request, err)
                    }
                }
                Err(err) => {
                    log::info!("Worker {} is shutting down, details: {:?}", self.id, err)
                }
            }
            self.free_sender.send(self.id).await.unwrap();
        }
    }
}

impl Server {
    pub async fn new(config: Configuration) -> Result<Server> {
        let graph_provider = graph_provider::gcloud::CloudStorageProvider::new(
            &*config.google_region,
            &*config.google_bucket,
            &*config.google_access_key,
            &*config.google_secret_key);

        let group_info = graph_provider.get_info(config.id).await.unwrap();

        let mut graphs = HashMap::new();
        for region_id in group_info.regions.iter() {
            graphs.insert(*region_id, graph_provider.get_region(*region_id).await.unwrap());
        }

        let redis_connector = redis_connector::RedisConnector::new(&*config.redis_url, config.redis_connection_count).await?;
        let zmq_listener = zmq_node::NodeListener::new(&*config.listen_addr).await?;
        let zmq_reply = zmq_node::ReplyConnector::new(&*config.reply_addr).await?;

        let network_mgr = redis_connector.get_servers_info().await?;

        let zmq_conn_mgr = zmq_node::NodeConnectionsManager::new(network_mgr.network_info).await?;

        let graphs = Arc::new(graphs);
        let mut workers = vec![];
        let mut task_senders = vec![];
        let (free_sender, free_receiver) = unbounded();
        for i in 0..config.worker_count {
            let (task_sender, task_receiver) = unbounded();
            let worker = Worker::new(
                redis_connector.clone(),
                graphs.clone(),
                zmq_reply.clone(),
                zmq_conn_mgr.clone(),
                task_receiver,
                free_sender.clone(),
                i
            ).await?;
            task_senders.push(task_sender);
            workers.push(tokio::task::spawn(async move { worker.work().await;}))
        }

        Ok(Server {
            zmq_listener,
            workers,
            task_senders,
            free_receiver
        })
    }

    pub async fn serve(&mut self) {
        loop {
            let worker_id = match self.free_receiver.recv().await {
                Ok(id) => { id }
                Err(err) => {
                    log::info!("Server is shutting down, details: {:?}", err);
                    continue;
                }
            };
            match self.zmq_listener.get_new_request().await {
                Ok(request) => {
                    if let Err(err) = self.task_senders[worker_id].send(request).await {
                        panic!("Unable to delegate job  to worker {}, error details: {}", worker_id, err)
                        // todo panic
                    }
                }
                Err(err) => {
                    match err {
                        ZMQError::ProtocolError(_) => {
                            panic!("{}", err)
                        }
                        _ => {
                            log::warn!("{}", err)
                        }
                    }
                }
            }
        }
    }
}