use std::collections::HashMap;
use std::fmt::Formatter;
use bitvec::vec::BitVec;
use serde::{Serialize, Deserialize};
use crate::domain::{NodeInfo, PathPoint};

pub type RegionIdx = u32;
pub type VertexIdx = usize;
pub type NodeIdx = usize;

#[derive(Debug, Clone)]
pub(crate) enum GraphError {
    NodeNotFound(NodeIdx, RegionIdx),
    VertexNotFound(VertexIdx, RegionIdx),
    NoVertexWithRegionBit(NodeIdx, RegionIdx, RegionIdx),
    MultipleVerticesWithRegionBit(NodeIdx, RegionIdx, RegionIdx, Vec<VertexIdx>),
}

impl std::fmt::Display for GraphError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        return match self {
            GraphError::NodeNotFound(node_id, region_id) => { write!(f, "Node {} cannot be found in region {}", node_id, region_id)}
            GraphError::VertexNotFound(vertex_id, region_id) => { write!(f, "Vertex {} cannot be found in region {}", vertex_id, region_id)}
            GraphError::NoVertexWithRegionBit(node_id, node_region, target_region) => { write!(f, "Node {} in region {} has no vertex with set bit for region {}", node_id, node_region, target_region)}
            GraphError::MultipleVerticesWithRegionBit(node_id, node_region, target_region, vertices) => { write!(f, "Node {} in region {} has multiple vertices with set bit for region {}, vertices {:?}", node_id, node_region, target_region, vertices)}
        }
    }
}

impl std::error::Error for GraphError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vertex {
    pub(crate) a: NodeIdx,
    pub(crate) b: NodeIdx,
    pub(crate) weight: u64,
    pub(crate) id: VertexIdx,
    pub(crate) region_bits: BitVec, // todo implement! (or check)
}

#[derive(Debug, Clone)]
pub struct Node {
    pub(crate) connections: Vec<VertexIdx>,
    pub(crate) id: NodeIdx,
    pub(crate) region: RegionIdx,
    pub(crate) cord_x: u64,
    pub(crate) cord_y: u64,
}

#[derive(Debug, Clone)]
pub struct Graph {
    pub(crate) nodes: HashMap<NodeIdx, Node>,
    vertices: HashMap<VertexIdx, Vertex>,
    pub(crate) region_idx: RegionIdx,
}

impl Vertex {
    fn get_neighbour(&self, a: NodeIdx) -> NodeIdx {
        if a == self.a {
            self.b
        } else if a == self.b {
            self.a
        } else {
            panic!("Invalid vertex chosen"); //todo
        }
    }
}

impl Node {
    pub(crate) fn new(connections: Vec<VertexIdx>,
                      id: NodeIdx,
                      region: RegionIdx,
                      cord_x: u64,
                      cord_y: u64) -> Self {
        Self {
            connections,
            id,
            region,
            cord_x,
            cord_y,
        }
    }
}

pub(crate) enum PathResult {
    TargetReached(Vec<PathPoint>, u64),
    Continue(Vec<PathPoint>, u64, RegionIdx),
}

impl Graph {
    pub(crate) fn new(nodes: HashMap<NodeIdx, Node>,
                      vertices: HashMap<VertexIdx, Vertex>,
                      region_idx: RegionIdx) -> Self {
        Self {
            nodes,
            vertices,
            region_idx,
        }
    }

    pub(crate) fn get_node(&self, idx: NodeIdx) -> Option<&Node> {
        self.nodes.get(&idx)
    }

    pub(crate) fn find_way(&self, source: NodeInfo, target: NodeInfo) -> Result<PathResult, GraphError> {
        let mut current_node = match self.nodes.get(&source.0) {
            Some(x) => { x }
            None => {
                return Err(GraphError::NodeNotFound(source.0, self.region_idx));
            }
        };

        let mut cost: u64 = 0;
        let mut path = vec![];
        while current_node.id != target.0 {
            let mut candidate_vertices = vec![];

            for vertex_id in current_node.connections.iter() {
                let vertex = match self.vertices.get(&vertex_id) {
                    Some(x) => { x }
                    None => {
                        return Err(GraphError::VertexNotFound(*vertex_id, self.region_idx));
                    }
                };
                if vertex.region_bits[target.1 as usize] {
                    candidate_vertices.push(vertex)
                }
            }

            let vertex = match candidate_vertices.len() {
                0 => {
                    return Err(GraphError::NoVertexWithRegionBit(
                        current_node.id,
                        self.region_idx,
                        target.1));
                }
                1 => {
                    *candidate_vertices.get(0).unwrap()
                }
                _ => {
                    let candidate_ids =
                        candidate_vertices.into_iter()
                            .map(|vertex| vertex.id).collect::<Vec<_>>();
                    return Err(GraphError::MultipleVerticesWithRegionBit(
                        current_node.id,
                        self.region_idx,
                        target.1,
                        candidate_ids));
                }
            };


            let new_node_id = vertex.get_neighbour(current_node.id);
            cost += vertex.weight;

            current_node = match self.nodes.get(&new_node_id) {
                Some(node) => {

                    let path_point = PathPoint::new(new_node_id, node.region, node.cord_x, node.cord_y);
                    path.push(path_point);

                    if node.region != self.region_idx {
                        return Ok(PathResult::Continue(path, cost, node.region));
                    }
                    node }
                None => {
                    return Err(GraphError::VertexNotFound(new_node_id, self.region_idx));
                }
            };
        }
        return Ok(PathResult::TargetReached(path, cost));
    }
}
