use crate::graph::{Node, NodeIdx};
use crate::RegionIdx;
use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct NodeInfo(pub(crate) NodeIdx, pub(crate) RegionIdx);

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Hash)]
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

impl From<Node> for PathPoint {
    fn from(node: Node) -> Self {
        Self::new(node.id,
                  node.region,
                  node.cord_x,
                  node.cord_y)
    }
}

impl PartialEq<Self> for PathPoint {
    fn eq(&self, other: &Self) -> bool {
        return self.id == other.id && self.region_id == other.region_id && self.cord_x == other.cord_x && self.cord_y == other.cord_y;
    }
}

impl Eq for PathPoint {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct PathRequest {
    pub(crate) request_id: usize,
    pub(crate) source: NodeInfo,
    pub(crate) target: NodeInfo,
    path: Vec<PathPoint>,
    cost: u64,
    pub(crate) visited_regions: Vec<RegionIdx>,
}

impl PathRequest {
    pub(crate) fn new(request_id: usize,
                      source: NodeInfo,
                      target: NodeInfo,
                      path: Vec<PathPoint>,
                      cost: u64,
                      visited_regions: Vec<RegionIdx>) -> PathRequest {
        PathRequest {
            request_id,
            source,
            target,
            path,
            cost,
            visited_regions,
        }
    }

    pub(crate) fn update_without_region(&self,
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
            self.visited_regions.clone(),
        )
    }
    pub(crate) fn update(&self,
                         mut path: Vec<PathPoint>,
                         cost: u64,
                         new_region_idx: RegionIdx) -> Self {
        let mut new_path = self.path.clone();
        new_path.append(&mut path);
        let mut visited_regions = self.visited_regions.clone();
        visited_regions.push(new_region_idx);

        PathRequest::new(
            self.request_id,
            self.source.clone(),
            self.target.clone(),
            new_path,
            self.cost.clone() + cost,
            visited_regions,
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
}

#[cfg(test)]
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
            visited_regions: vec![],
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