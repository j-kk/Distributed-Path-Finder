use std::env;
use pathfinder::{Configuration, Context, Server};

#[tokio::main]
async fn main() {
    env_logger::init();
    log::info!("Pathfinder launching!");
    for (key, value) in env::vars() {
        eprintln!("{}: {}", key, value);
    }
    let config = Configuration::from_env().unwrap();
    let context = if env::var("ZMQ_MODE").is_ok() {
        log::info!("Launching in ZMQ mode");
        Context::zmq_ctx(&config).await.unwrap()
    } else {
        log::info!("Launching in Redis mode");
        Context::redis_ctx(&config).await.unwrap()
    };

    let mut server = Server::new(config, context).await.unwrap();
    server.serve().await;
}
