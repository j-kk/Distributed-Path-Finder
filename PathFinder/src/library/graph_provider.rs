use serde::{Serialize, Deserialize};
use crate::graph::{Graph, Node, NodeIdx, RegionIdx, Vertex};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawNode {
    id: NodeIdx,
    cord_x: u64,
    cord_y: u64,
}

impl From<RawNode> for Node {
    fn from(raw_node: RawNode) -> Self {
        return Node::new(
            vec![],
            raw_node.id,
            raw_node.cord_x,
            raw_node.cord_y,
        );
    }
}


#[async_trait::async_trait]
pub trait GraphProvider {
    async fn get_region(&self, id: RegionIdx) -> Result<Graph>;
}

mod mock {
    use std::collections::HashMap;
    use std::path::{PathBuf};
    use futures_util::StreamExt;
    use crate::graph_provider::{Graph, GraphProvider, Node, RawNode, Result, Vertex};
    use crate::graph::RegionIdx;

    struct MockGraphProvider {
        dir_path: PathBuf,
    }

    impl MockGraphProvider {
        fn new(dir_path: PathBuf) -> Self {
            Self {
                dir_path
            }
        }
    }

    #[async_trait::async_trait]
    impl GraphProvider for MockGraphProvider {
        async fn get_region(&self, id: RegionIdx) -> Result<Graph> {
            let vertex_filepath = self.dir_path.clone().join(format!("region_{}", id));
            let nodes_filepath = self.dir_path.clone().join(format!("region_{}", id));


            let nodes_file = tokio::fs::File::open(nodes_filepath).await?;
            let mut nodes_reader = csv_async::AsyncDeserializer::from_reader(nodes_file);
            let mut nodes = HashMap::new();
            let mut nodes_read = nodes_reader.deserialize::<RawNode>();
            while let Some(record) = nodes_read.next().await {
                let raw_node = record?;
                let node = Node::from(raw_node);
                nodes.insert(node.id, node);
            }

            let vertex_file = tokio::fs::File::open(vertex_filepath).await?;
            let mut vertices_reader = csv_async::AsyncDeserializer::from_reader(vertex_file);
            let mut vertices = HashMap::new();
            let mut vertices_read = vertices_reader.deserialize::<Vertex>();
            while let Some(record) = vertices_read.next().await {
                let record = record?;
                nodes.get_mut(&record.a).map(|node| node.connections.push(record.id));
                nodes.get_mut(&record.b).map(|node| node.connections.push(record.id));
                vertices.insert(record.id, record);
            }

            return Ok(Graph::new(
                nodes,
                vertices,
                id,
            ));
        }
    }
}


pub mod gcloud {
    use std::collections::HashMap;
    use std::env;
    use std::io::Error;
    use std::io::ErrorKind::{NotFound};
    use s3::{Bucket, Region};
    use s3::creds::Credentials;
    use crate::graph_provider::{Graph, GraphProvider, Node, RawNode, Result, Vertex};
    use crate::graph::RegionIdx;

    pub struct CloudStorageProvider {
        bucket: Bucket,
    }

    impl CloudStorageProvider {
        pub fn new(region: &str,
                   bucket: &str,
                   access_key: &str,
                   secret_key: &str) -> Self {
            let region = Region::Custom {
                region: region.to_owned(),
                endpoint: "https://storage.googleapis.com".to_owned(),
            };
            let bucket = Bucket::new(bucket,
                                     region,
                                     Credentials::new(
                                         Some(access_key),
                                         Some(secret_key),
                                         None,
                                         None,
                                         None,
                                     ).unwrap()).unwrap();
            return Self {
                bucket
            };
        }

        pub fn from_env() -> Self {
            Self::new(
                &*env::var("GOOGLE_CLOUD_REGION").unwrap(),
                &*env::var("GOOGLE_CLOUD_BUCKET").unwrap(),
                &*env::var("GOOGLE_CLOUD_ACCESS_KEY").unwrap(),
                &*env::var("GOOGLE_CLOUD_SECRET_KEY").unwrap(),
            )
        }
    }

    #[async_trait::async_trait]
    impl GraphProvider for CloudStorageProvider {
        async fn get_region(&self, id: RegionIdx) -> Result<Graph> {
            let (nodes_data, return_code) = self.bucket.get_object(format!("nodes_{}", id)).await?;
            if !(200 <= return_code && return_code < 300) {
                return Err(Box::new(Error::from(NotFound)));
            }
            let mut nodes_reader = csv::Reader::from_reader(&*nodes_data);
            let mut nodes = HashMap::new();
            let mut nodes_read = nodes_reader.deserialize::<RawNode>();
            while let Some(record) = nodes_read.next() {
                let raw_node = record?;
                let node = Node::from(raw_node);
                nodes.insert(node.id, node);
            }

            let (vertices_data, return_code) = self.bucket.get_object(format!("vertices_{}", id)).await?;
            if !(200 <= return_code && return_code < 300) {
                return Err(Box::new(Error::from(NotFound)));
            }
            let mut vertices_reader = csv::Reader::from_reader(&*vertices_data);
            let mut vertices = HashMap::new();
            let mut vertices_read = vertices_reader.deserialize::<Vertex>();
            while let Some(record) = vertices_read.next() {
                let record = record?;
                nodes.get_mut(&record.a).map(|node| node.connections.push(record.id));
                nodes.get_mut(&record.b).map(|node| node.connections.push(record.id));
                vertices.insert(record.id, record);
            }

            return Ok(Graph::new(
                nodes,
                vertices,
                id,
            ));
        }
    }
}