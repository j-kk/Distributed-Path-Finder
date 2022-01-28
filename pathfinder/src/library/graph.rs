use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use bitvec::vec::BitVec;
use priority_queue::PriorityQueue;
use serde::{Serialize, Deserialize};
use crate::domain::{NodeInfo, PathPoint};
use crate::PathResult::Continue;

pub type RegionIdx = u32;
pub type VertexIdx = usize;
pub type NodeIdx = usize;

#[derive(Debug, Clone)]
pub(crate) enum GraphError {
    NodeNotFound(NodeIdx, RegionIdx),
    VertexNotFound(VertexIdx, RegionIdx),
    NoVertexWithRegionBit(NodeIdx, RegionIdx, RegionIdx),
    MultipleVerticesWithRegionBit(NodeIdx, RegionIdx, RegionIdx, Vec<VertexIdx>),
    Unreachable(NodeIdx, RegionIdx),
}

impl std::fmt::Display for GraphError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        return match self {
            GraphError::NodeNotFound(node_id, region_id) => { write!(f, "Node {} cannot be found in region {}", node_id, region_id) }
            GraphError::VertexNotFound(vertex_id, region_id) => { write!(f, "Vertex {} cannot be found in region {}", vertex_id, region_id) }
            GraphError::NoVertexWithRegionBit(node_id, node_region, target_region) => { write!(f, "Node {} in region {} has no vertex with set bit for region {}", node_id, node_region, target_region) }
            GraphError::MultipleVerticesWithRegionBit(node_id, node_region, target_region, vertices) => { write!(f, "Node {} in region {} has multiple vertices with set bit for region {}, vertices {:?}", node_id, node_region, target_region, vertices) }
            GraphError::Unreachable(vertex_id, region_id) => { write!(f, "Vertex {} cannot reached in region {}", vertex_id, region_id) }
        };
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

    pub(crate) fn find_way_local(&self, source: NodeInfo,
                                 target: NodeInfo) -> Result<PathResult, GraphError> {
        let mut queue: PriorityQueue<(NodeIdx, Vec<PathPoint>), i64> = PriorityQueue::new();
        let mut visited = HashSet::new();
        let start_node = self.nodes.get(&source.0).ok_or(GraphError::NodeNotFound(source.0, self.region_idx))?;
        queue.push((start_node.id, vec![]), 0);

        while queue.len() > 0 {
            let ((node_idx, path), mut cost): ((NodeIdx, Vec<PathPoint>), i64) = queue.pop().unwrap();
            cost *= -1;
            let node = self.nodes.get(&node_idx).ok_or(GraphError::NodeNotFound(node_idx, self.region_idx))?;
            if node.id == target.0 {
                return Ok(PathResult::TargetReached(path, cost as u64));
            }
            for vertex_id in node.connections.iter() {
                let vertex = self.vertices.get(&vertex_id).ok_or(GraphError::VertexNotFound(*vertex_id, self.region_idx))?;
                let next = vertex.get_neighbour(node.id);
                if !visited.contains(&next) {
                    if let Some(next_node) = self.nodes.get(&next) {
                        visited.insert(next);
                        let mut new_path = path.clone();
                        new_path.push(PathPoint::from(next_node.clone()));
                        queue.push((next_node.id, new_path), -(cost + vertex.weight as i64));
                    }
                }
            }
        }

        Err(GraphError::Unreachable(target.0, target.1))
    }

    pub(crate) fn find_way(&self, source: NodeInfo, target: NodeInfo) -> Result<Vec<PathResult>, GraphError> {
        let mut queue: PriorityQueue<(NodeIdx, Vec<PathPoint>), u64> = PriorityQueue::new();
        let mut possibilities = vec![];
        let mut visited = HashSet::new();
        let start_node = self.nodes.get(&source.0).ok_or(GraphError::NodeNotFound(source.0, self.region_idx))?;
        queue.push((start_node.id, vec![]), 0);

        while queue.len() > 0 {
            let ((node_idx, path), cost): ((NodeIdx, Vec<PathPoint>), u64) = queue.pop().unwrap();
            let node = self.nodes.get(&node_idx).ok_or(GraphError::NodeNotFound(node_idx, self.region_idx))?;
            if self.region_idx != node.region {
                possibilities.push(Continue(path, cost, node.region));
                continue;
            }

            for vertex_id in node.connections.iter() {
                let vertex = self.vertices.get(&vertex_id).ok_or(GraphError::VertexNotFound(*vertex_id, self.region_idx))?;
                if vertex.region_bits[target.1 as usize] {
                    let next = vertex.get_neighbour(node.id);
                    if !visited.contains(&next) {
                        let next_node = self.nodes.get(&next).ok_or(GraphError::NodeNotFound(next, self.region_idx))?;
                        visited.insert(next);
                        let mut new_path = path.clone();
                        new_path.push(PathPoint::from(next_node.clone()));
                        queue.push((next_node.id, new_path), cost + vertex.weight);
                    }
                }
            }
        }
        Ok(possibilities)
    }
}
