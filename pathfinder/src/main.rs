use std::env;
use pathfinder::{Configuration, Context, Server};

#[tokio::main]
async fn main() {
    env_logger::init();
    let config = Configuration::from_env().unwrap();
    let context = if env::var("ZMQ_MODE").is_ok() {
        Context::zmq_ctx(&config).await.unwrap()
    } else {
        Context::redis_ctx(&config).await.unwrap()
    };

    let mut server = Server::new(config, context).await.unwrap();
    server.serve().await;
}
