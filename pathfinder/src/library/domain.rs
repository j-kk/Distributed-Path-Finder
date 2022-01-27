use crate::graph::NodeIdx;
use crate::RegionIdx;
use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct NodeInfo(pub(crate) NodeIdx, pub(crate) RegionIdx);

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct PathPoint {
    id: NodeIdx,
    region_id: RegionIdx,
    cord_x: u64,
    cord_y: u64,
}

impl PathPoint {
    pub(crate) fn new(id: NodeIdx,
                      region_id: RegionIdx,
                      cord_x: u64,
                      cord_y: u64) -> Self {
        Self {
            id,
            region_id,
            cord_x,
            cord_y,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct PathRequest {
    pub(crate) request_id: usize,
    pub(crate) source: NodeInfo,
    pub(crate) target: NodeInfo,
    path: Vec<PathPoint>,
    cost: u64,
}

impl PathRequest {
    pub(crate) fn new(request_id: usize,
                      source: NodeInfo,
                      target: NodeInfo,
                      path: Vec<PathPoint>,
                      cost: u64) -> PathRequest {
        PathRequest {
            request_id,
            source,
            target,
            path,
            cost,
        }
    }

    pub(crate) fn update(&self,
                         mut path: Vec<PathPoint>,
                         cost: u64) -> Self {
        let mut new_path = self.path.clone();
        new_path.append(&mut path);
        PathRequest::new(
            self.request_id,
            self.source.clone(),
            self.target.clone(),
            new_path,
            self.cost.clone() + cost,
        )
    }

    pub(crate) fn get_last_node(&self) -> Option<NodeInfo> {
        let node = if self.path.len() > 0 {
            &self.path[self.path.len() - 1]
        } else {
            return None;
        };
        Some(NodeInfo(node.id, node.region_id))
    }


}#[cfg(test)]
mod test {
    use crate::{PathPoint, PathRequest};
    use crate::domain::NodeInfo;

    #[tokio::test]
    async fn sample_request() {
        let mut request = PathRequest {
            request_id: 12,
            source: NodeInfo(1, 1),
            target: NodeInfo(100, 10),
            path: vec![],
            cost: 0,
        };
        let serialized_empty = serde_json::to_string(&request).unwrap();
        println!("{}", serialized_empty);

        let p1 = PathPoint {
            id: 2,
            region_id: 1,
            cord_x: 10,
            cord_y: 0,
        };


        let p2 = PathPoint {
            id: 3,
            region_id: 1,
            cord_x: 10,
            cord_y: 3,
        };
        request.path.push(p1);
        request.path.push(p2);

        let serialized_empty = serde_json::to_string(&request).unwrap();
        println!("{}", serialized_empty);

    }
}