use std::sync::Arc;
use async_channel::{Receiver, Sender};
use tokio::task::JoinHandle;
use crate::graph::{Graph, PathResult};
use crate::graph_provider::GraphProvider;
use crate::redis_connector::RedisConnector;
use crate::zmq_node::{NodeConnectionsManager, NodeListener, PathRequest, ReplyConnector, ZMQError};

mod zmq_node;
mod graph;
mod redis_connector;
pub mod graph_provider;
mod error;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Clone)]
struct Configuration {
    region: String,
    bucket: String,
    google_access_key: String,
    google_secret_key: String,
    id: usize,
}

struct Server {
    graph_provider: Box<dyn GraphProvider>,
    graph: Graph,
    zmq_listener: Arc<NodeListener>,
    zmq_conn_mgr: NodeConnectionsManager,
    zmq_reply: ReplyConnector,
    redis_connector: RedisConnector,
    workers: Vec<JoinHandle<()>>,
    task_sender: Vec<Sender<PathRequest>>,
    free_receiver: Receiver<usize>,
}

struct Worker {
    redis_connector: RedisConnector,
    graph: Arc<Graph>,
    zmq_reply: ReplyConnector,
    zmq_conn_mgr: NodeConnectionsManager,
    task_receiver: Receiver<PathRequest>,
    free_sender: Sender<usize>,
    id: usize,
}

impl Worker {
    async fn new(redis_connector: RedisConnector,
                 graph: Arc<Graph>,
                 zmq_reply: ReplyConnector,
                 zmq_conn_mgr: NodeConnectionsManager,
                 task_receiver: Receiver<PathRequest>,
                 free_sender: Sender<usize>,
                 id: usize) -> Result<Worker> {
        free_sender.send(id).await;
        Ok(Worker {
            redis_connector,
            graph,
            zmq_reply,
            zmq_conn_mgr,
            task_receiver,
            free_sender,
            id,
        })
    }

    async fn serve_request(&self, request: &PathRequest) -> Result<()> {
        let start_node = request.get_last_node();
        let path_result = self.graph.find_way(start_node, request.target)?;
        match path_result {
            PathResult::TargetReached(path, cost) => {
                let reply = request.update(path, cost);
                self.zmq_reply.send(&reply).await?;
            }
            PathResult::Continue(path, cost, next_node) => {
                let new_request = request.update(path, cost);
                let next_region = self.redis_connector.get_region(next_node).await?;
                let server_id = self.redis_connector.get_server_id(next_region).await?;
                self.zmq_conn_mgr.send_request(server_id, new_request);
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
        }
    }
}

impl Server {

    async fn serve(&self) {
        loop {
            let worker_id = match self.free_receiver.recv().await {
                Ok(id) => { id }
                Err(err) => {
                    log::info!("Server is shutting down, details: {:?}", err);
                    continue
                }
            };
            match self.zmq_listener.get_new_request() {
                Ok(request) => {
                    if let Err(err) = self.task_sender[worker_id].send(request).await {
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