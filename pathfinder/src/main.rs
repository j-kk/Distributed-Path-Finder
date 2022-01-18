use pathfinder::{Configuration, Server};

#[tokio::main]
async fn main() {
    let config = Configuration::from_env().unwrap();
    let mut server = Server::new(config).await.unwrap();
    server.serve().await;
}
