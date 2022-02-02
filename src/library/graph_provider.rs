use bitvec::vec::BitVec;
use serde::{Serialize, Deserialize};
use crate::graph::{Graph, Node, NodeIdx, RegionIdx, Vertex, VertexIdx};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawNode {
    id: NodeIdx,
    cord_x: u64,
    cord_y: u64,
    region: RegionIdx,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawVertex {
    pub(crate) id: VertexIdx,
    pub(crate) a: NodeIdx,
    pub(crate) b: NodeIdx,
    pub(crate) weight: u64,
    region_bits: String,
}

impl From<RawNode> for Node {
    fn from(raw_node: RawNode) -> Self {
        return Node::new(
            vec![],
            raw_node.id,
            raw_node.region,
            raw_node.cord_x,
            raw_node.cord_y,
        );
    }
}

impl From<RawVertex> for Vertex {
    fn from(raw_vertex: RawVertex) -> Self {
        let char_vec = raw_vertex.region_bits.chars().collect::<Vec<_>>();
        let bool_vec = char_vec.into_iter().map(|c| match c {
            '0' => { false }
            '1' => { true }
            x => panic!("Bitvec has unknown character: {}", x)
        });
        Self {
            a: raw_vertex.a,
            b: raw_vertex.b,
            weight: raw_vertex.weight,
            id: raw_vertex.id,
            region_bits: BitVec::from_iter(bool_vec),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GroupInfo {
    pub(crate) group_id: usize,
    pub(crate) regions: Vec<RegionIdx>,
}

#[async_trait::async_trait]
pub trait GraphProvider {
    async fn get_region(&self, id: RegionIdx) -> Result<Graph>;
}

#[async_trait::async_trait]
pub(crate) trait GroupInfoProvider {
    async fn get_info(&self, group_id: usize) -> Result<GroupInfo>;
}

pub mod mock {
    use std::collections::HashMap;
    use std::path::{PathBuf};
    use futures_util::StreamExt;
    use tokio::io::AsyncReadExt;
    use crate::graph_provider::{Graph, GraphProvider, GroupInfo, Node, RawNode, RawVertex, Result, Vertex};
    use crate::graph::RegionIdx;
    use crate::GroupInfoProvider;

    pub(crate) struct MockGraphProvider {
        dir_path: PathBuf,
    }

    impl MockGraphProvider {
        pub(crate) fn new(dir_path: PathBuf) -> Self {
            Self {
                dir_path
            }
        }
    }

    #[async_trait::async_trait]
    impl GraphProvider for MockGraphProvider {
        async fn get_region(&self, id: RegionIdx) -> Result<Graph> {
            let vertex_filepath = self.dir_path.clone().join(format!("vertices/vertices_{}.csv", id));
            let nodes_filepath = self.dir_path.clone().join(format!("nodes/nodes_{}.csv", id));
            assert!(vertex_filepath.exists());
            assert!(nodes_filepath.exists());

            let nodes_file = tokio::fs::File::open(nodes_filepath).await?;
            let mut nodes_reader = csv_async::AsyncReaderBuilder::new().has_headers(false).create_deserializer(nodes_file);
            let mut nodes = HashMap::new();
            let mut nodes_read = nodes_reader.deserialize::<RawNode>();
            while let Some(record) = nodes_read.next().await {
                let raw_node = record?;
                let node = Node::from(raw_node);
                nodes.insert(node.id, node);
            }

            let vertex_file = tokio::fs::File::open(vertex_filepath).await?;
            let mut vertices_reader = csv_async::AsyncReaderBuilder::new().has_headers(false).create_deserializer(vertex_file);
            let mut vertices = HashMap::new();
            let mut vertices_read = vertices_reader.deserialize::<RawVertex>();
            while let Some(record) = vertices_read.next().await {
                let record = record?;
                let vertex = Vertex::from(record);
                nodes.get_mut(&vertex.a).map(|node| node.connections.push(vertex.id));
                nodes.get_mut(&vertex.b).map(|node| node.connections.push(vertex.id));
                vertices.insert(vertex.id, vertex);
            }

            return Ok(Graph::new(
                nodes,
                vertices,
                id,
            ));
        }
    }

    #[async_trait::async_trait]
    impl GroupInfoProvider for MockGraphProvider {
        async fn get_info(&self, group_id: usize) -> Result<GroupInfo> {
            let nodes_filepath = self.dir_path.clone().join(format!("group_{}.json", group_id));
            assert!(nodes_filepath.exists());
            let mut nodes_file = tokio::fs::File::open(nodes_filepath).await?;
            let mut content = vec![];
            nodes_file.read_buf(&mut content).await?;
            Ok(serde_json::from_slice::<GroupInfo>(&*content)?)
        }
    }

    #[cfg(test)]
    mod test {
        use std::path::PathBuf;
        use crate::graph_provider::mock::MockGraphProvider;
        use crate::{GraphProvider, GroupInfoProvider};

        #[tokio::test]
        async fn test_group_info() {
            let provider = MockGraphProvider::new(PathBuf::from("res/groups/"));
            let group_info = provider.get_info(2).await.unwrap();
            assert_eq!(group_info.group_id, 2);
            assert!(group_info.regions.len() > 0);
        }

        #[tokio::test]
        async fn test_graph() {
            let provider = MockGraphProvider::new(PathBuf::from("res/"));
            let graph = provider.get_region(1).await.unwrap();
            assert_eq!(graph.region_idx, 1);
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
    use crate::graph_provider::{Graph, GraphProvider, GroupInfo, GroupInfoProvider, Node, RawNode, RawVertex, Result, Vertex};
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
                endpoint: "http://storage.googleapis.com".to_owned(),
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
                &*env::var("GOOGLE_ACCESS_KEY").unwrap(),
                &*env::var("GOOGLE_SECRET_KEY").unwrap(),
            )
        }
    }

    #[async_trait::async_trait]
    impl GraphProvider for CloudStorageProvider {
        async fn get_region(&self, id: RegionIdx) -> Result<Graph> {
            log::info!("Retrieving region data {}", id);
            let (nodes_data, return_code) = self.bucket.get_object(format!("nodes_{}.csv", id)).await?;
            if !(200 <= return_code && return_code < 300) {
                return Err(Box::new(Error::from(NotFound)));
            }

            let mut nodes_reader = csv::ReaderBuilder::new().has_headers(false).from_reader(&*nodes_data);
            let mut nodes = HashMap::new();
            let mut nodes_read = nodes_reader.deserialize::<RawNode>();
            while let Some(record) = nodes_read.next() {
                let raw_node = record?;
                let node = Node::from(raw_node);
                nodes.insert(node.id, node);
            }

            let (vertices_data, return_code) = self.bucket.get_object(format!("vertices_{}.csv", id)).await?;
            if !(200 <= return_code && return_code < 300) {
                return Err(Box::new(Error::from(NotFound)));
            }

            let mut vertices_reader = csv::ReaderBuilder::new().has_headers(false).from_reader(&*vertices_data);
            let mut vertices = HashMap::new();
            let mut vertices_read = vertices_reader.deserialize::<RawVertex>();
            while let Some(record) = vertices_read.next() {
                let record = record?;
                let vertex = Vertex::from(record);
                nodes.get_mut(&vertex.a).map(|node| node.connections.push(vertex.id));
                nodes.get_mut(&vertex.b).map(|node| node.connections.push(vertex.id));
                vertices.insert(vertex.id, vertex);
            }

            return Ok(Graph::new(
                nodes,
                vertices,
                id,
            ));
        }
    }

    #[async_trait::async_trait]
    impl GroupInfoProvider for CloudStorageProvider {
        async fn get_info(&self, group_id: usize) -> Result<GroupInfo> {
            let (group_raw, return_code) = self.bucket.get_object(format!("group_{}.json", group_id)).await?;
            if !(200 <= return_code && return_code < 300) {
                let body: String = String::from_utf8(group_raw).unwrap_or(String::from("???"));
                log::error!("Cloud storage returned {}: {}", return_code, body);
                return Err(Box::new(Error::from(NotFound)));
            }
            Ok(serde_json::from_slice::<GroupInfo>(&*group_raw)?)
        }
    }

    #[cfg(test)]
    mod test {
        use crate::graph_provider::gcloud::CloudStorageProvider;
        use crate::{GraphProvider, GroupInfoProvider};

        #[tokio::test]
        async fn test_get_group() {
            let cloud = CloudStorageProvider::from_env();
            cloud.get_info(2).await.unwrap();
            cloud.get_region(1).await.unwrap();
        }
    }
}