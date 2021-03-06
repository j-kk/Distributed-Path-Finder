use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use async_channel::{Receiver, Sender, unbounded};
use tokio::task::JoinHandle;
use crate::domain::{NodeInfo, PathRequest};
use crate::graph::{Continuation, Graph, GraphError, PathResult, RegionIdx};
use crate::graph_provider::{GraphProvider, GroupInfoProvider};
use crate::redis_connector::{RedisConnector};
use crate::node_connector::{NodeSender, ResultReplier, ConnectionError, NodeListener};

mod node_connector;
mod graph;
mod redis_connector;
pub mod graph_provider;
mod domain;

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
    worker_count: usize,
}

impl Configuration {
    pub fn from_env() -> Result<Configuration> {
        let id: usize = match env::var("GROUP_ID") {
            Ok(s) => {
                log::debug!("Got ID from env var {}", s);
                s.parse()?
            }
            Err(_) => {
                match env::var("HOSTNAME") {
                    Ok(s) => {
                        log::debug!("Decoding ID from hostname {}", s);
                        let splitted: Vec<&str> = s.split('-').collect();
                        log::debug!("Got ID from hostname {}", splitted[1]);
                        splitted[1].parse()?
                    }
                    Err(err) => {
                        log::error!("No ID given");
                        return Err(Box::new(err));
                    }
                }
            }
        };
        let redis_url = match env::var("REDIS_URL") {
            Ok(url) => { url }
            Err(_) => {
                match env::var("REDIS_SERVICE_HOST") {
                    Ok(url) => { format!("redis://{}:6379", url) }
                    Err(err) => {
                        log::error!("No redis url given");
                        return Err(Box::new(err));
                    }
                }
            }
        };


        Ok(Configuration {
            google_region: env::var("GOOGLE_CLOUD_REGION")?,
            google_bucket: env::var("GOOGLE_CLOUD_BUCKET")?,
            google_access_key: env::var("GOOGLE_ACCESS_KEY")?,
            google_secret_key: env::var("GOOGLE_SECRET_KEY")?,
            id,
            redis_url,
            redis_connection_count: env::var("REDIS_CONNECTION_COUNT")?.parse()?,
            worker_count: env::var("WORKER_COUNT")?.parse()?,
        })
    }
}

pub struct Context {
    result_reply: Box<dyn ResultReplier>,
    node_listener: Box<dyn NodeListener>,
    node_sender_mgr: Box<dyn NodeSender>,
    redis_connector: RedisConnector,
}

impl Context {
    pub async fn redis_ctx(config: &Configuration) -> Result<Context> {
        let redis_connector = redis_connector::RedisConnector::new(&*config.redis_url, config.redis_connection_count).await?;
        let node_listener = Box::new(node_connector::redis_connector::RedisNodeListener::new(&redis_connector, config.id).await?);
        let result_reply = Box::new(node_connector::redis_connector::RedisReplier::new(redis_connector.clone()).await?);

        let node_sender_mgr = Box::new(node_connector::redis_connector::RedisConnectionsManager::new(redis_connector.clone()).await?);
        Ok(Context {
            redis_connector,
            result_reply,
            node_listener,
            node_sender_mgr,
        })
    }

    pub async fn zmq_ctx(config: &Configuration) -> Result<Context> {
        let listen_addr = env::var("LISTEN_ADDR")?;
        let reply_addr = env::var("REPLY_ADDR")?;

        let redis_connector = redis_connector::RedisConnector::new(&*config.redis_url, config.redis_connection_count).await?;
        let node_listener = Box::new(node_connector::zmq_connector::ZMQNodeListener::new(&*listen_addr).await?);
        let result_reply = Box::new(node_connector::zmq_connector::ZMQReplier::new(&*reply_addr).await?);

        let network_mgr = redis_connector.get_servers_info().await?;

        let node_sender_mgr = Box::new(node_connector::zmq_connector::ZMQConnectionsManager::new(network_mgr.network_info).await?);
        Ok(Context {
            redis_connector,
            result_reply,
            node_listener,
            node_sender_mgr,
        })
    }
}


pub struct Server {
    node_listener: Box<dyn NodeListener>,
    workers: Vec<JoinHandle<()>>,
    task_senders: Vec<Sender<PathRequest>>,
    free_receiver: Receiver<usize>,
    free_sender: Sender<usize>,
}

struct Worker {
    redis_connector: RedisConnector,
    graphs: Arc<HashMap<RegionIdx, Graph>>,
    result_reply: Box<dyn ResultReplier>,
    node_sender_mgr: Box<dyn NodeSender>,
    task_receiver: Receiver<PathRequest>,
    free_sender: Sender<usize>,
    id: usize,
}

impl Worker {
    async fn new(redis_connector: RedisConnector,
                 graphs: Arc<HashMap<RegionIdx, Graph>>,
                 zmq_reply: Box<dyn ResultReplier>,
                 zmq_conn_mgr: Box<dyn NodeSender>,
                 task_receiver: Receiver<PathRequest>,
                 free_sender: Sender<usize>,
                 id: usize) -> Result<Worker> {
        free_sender.send(id).await?;
        Ok(Worker {
            redis_connector,
            graphs,
            result_reply: zmq_reply,
            node_sender_mgr: zmq_conn_mgr,
            task_receiver,
            free_sender,
            id,
        })
    }

    async fn serve_request(&self, request: &PathRequest) -> Result<()> {
        let mut start_region = None;
        for (region_idx, graph) in self.graphs.iter() {
            if graph.get_node(request.last).is_some() {
                start_region = Some(region_idx);
            }
        }
        let start_region = match start_region {
            Some(r) => {r}
            None => {
                log::warn!("Received request to node {}, however this worker does not serve it's region. Request: {:?}", request.last, request);
                Err("Not served region")?
            }
        };

        let graph = self.graphs.get(&start_region).ok_or(GraphError::StartNodeNotFound(request.last, *start_region))?;
        let path_results: Vec<PathResult> = if request.target.1 == *start_region {
            vec![graph.find_way_local(NodeInfo(request.last, *start_region), request.target)?]
        } else {
            graph.find_way(NodeInfo(request.last, *start_region), request.target)? // todo
        };
        let mut to_send: Vec<(usize, PathRequest)> = vec![];
        for path_result in path_results.into_iter() {
            match path_result {
                PathResult::TargetReached(path, cost) => {
                    let reply = request.update_without_region(path, request.target.0, cost);
                    log::debug!("Target reached! Sending over the result. Request id: {}, total cost: {}", request.request_id, cost);
                    self.result_reply.send(&reply).await?;
                    return Ok(())
                }
                PathResult::Continue(path, cost, continuation) => {
                    let next_region = match continuation {
                        Continuation::CRegionKnown(_, region) => {region}
                        Continuation::CRegionUnknown(node_idx) => {self.redis_connector.get_region(node_idx).await?}
                    };
                    if !request.visited_regions.contains(&next_region) {
                        let new_request = request.update(path, continuation.get_node_idx(), cost, next_region);
                        let server_id = self.redis_connector.get_server_id(next_region).await?;
                        log::debug!("Reached region boundary. Sending over the request to server {}. Request id: {}, total cost: {}", server_id, request.request_id, cost);
                        to_send.push((server_id, new_request));
                    } else {
                        log::debug!("Skipping request to {} (region has been already visited)", next_region);
                    }
                }
            }
        }
        for (server_id, new_request) in to_send.into_iter() {
            self.node_sender_mgr.send_request(server_id, new_request).await?;
        }
        Ok(())
    }

    async fn work(&self) {
        self.free_sender.send(self.id).await.unwrap();
        loop {
            match self.task_receiver.recv().await {
                Ok(request) => {
                    if let Err(err) = self.serve_request(&request).await {
                        log::warn!("Worker {} couldn't handle request {:?}, details: {:?}", self.id, request, err)
                    }
                }
                Err(err) => {
                    log::warn!("Worker {} is shutting down, details: {:?}", self.id, err)
                }
            }
            self.free_sender.send(self.id).await.unwrap();
        }
    }
}

impl Server {
    pub async fn new(config: Configuration, context: Context) -> Result<Server> {
        let graph_provider = graph_provider::gcloud::CloudStorageProvider::new(
            &*config.google_region,
            &*config.google_bucket,
            &*config.google_access_key,
            &*config.google_secret_key);

        let group_info = graph_provider.get_info(config.id).await.unwrap();

        let mut graphs = HashMap::new();
        for region_id in group_info.regions.iter() {
            log::info!("Loading region {}", region_id);
            let graph = graph_provider.get_region(*region_id).await.unwrap();
            context.redis_connector.set_group(*region_id, group_info.group_id).await?;
            context.redis_connector.set_region(&graph, *region_id).await?;
            graphs.insert(*region_id, graph);
            log::debug!("Region {} successfully loaded", region_id);
        }


        let graphs = Arc::new(graphs);
        let mut workers = vec![];
        let mut task_senders = vec![];
        let (free_sender, free_receiver) = unbounded();
        for i in 0..config.worker_count {
            let (task_sender, task_receiver) = unbounded();
            let worker = Worker::new(
                context.redis_connector.clone(),
                graphs.clone(),
                context.result_reply.clone(),
                context.node_sender_mgr.clone(),
                task_receiver,
                free_sender.clone(),
                i,
            ).await?;
            task_senders.push(task_sender);
            workers.push(tokio::task::spawn(async move { worker.work().await }));
            log::debug!("Worker spawned {}", i);
        }
        log::info!("Ready to work!");
        Ok(Server {
            node_listener: context.node_listener,
            workers,
            task_senders,
            free_receiver,
            free_sender
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
            log::debug!("Got free worker {}", worker_id);
            match self.node_listener.get_new_request().await {
                Ok(request) => {
                    log::info!("Dispatching request with id {} to worker {}", request.request_id, worker_id);
                    if let Err(err) = self.task_senders[worker_id].send(request).await {
                        panic!("Unable to delegate job  to worker {}, error details: {}", worker_id, err)
                    }
                }
                Err(err) => {
                    match err {
                        ConnectionError::ProtocolError(_) => {
                            panic!("{}", err)
                        }
                        _ => {
                            self.free_sender.send(worker_id).await.unwrap();
                            log::warn!("{}", err)
                        }
                    }
                }
            }
        }
    }
}