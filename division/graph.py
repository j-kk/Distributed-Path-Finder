import os
from typing import Iterable

from geometry import Point2D, Rectangle2D



class Vertex:
    def __init__(self, id: int, location: Point2D):
        self.id = id
        self.location = location
        self.edges = dict[int, Edge]()

    def __str__(self) -> str:
        return f'<Vertex id:{self.id} location:{self.location}>'

    def add_edge(self, edge: 'Edge') -> None:
        self.edges[edge.id] = edge

    def get_edges(self) -> Iterable['Edge']:
        return self.edges.values()


class Edge:
    def __init__(self, id: int, vert1: Vertex, vert2: Vertex, weight: int):
        self.id = id
        self.vert1 = vert1
        self.vert2 = vert2
        self.weight = weight
        
    def __str__(self) -> str:
        return f'<Edge id:{self.id} vert1:{self.vert1} vert2:{self.vert2} weight:{self.weight}>'

    def get_other_vert(self, vert) -> Vertex:
        return self.vert1 if vert == self.vert2 else self.vert2


class Graph2D:
    def __init__(self):
        self.vertices = dict[int, Vertex]()
        self.edges = dict[int, Edge]()
        self.bounds: Rectangle2D = None

    def __str__(self) -> str:
        vertStr = ""
        for vert in self.vertices.values():
            vertStr += str(vert) + os.linesep + "\t"
        edgeStr = ""
        for edge in self.edges.values():
            edgeStr += str(edge) + os.linesep + "\t"
        return "<Graph2D bounds:{} vertices:[{}] edges:[{}]".format(self.bounds, vertStr, edgeStr)

    def add_vertex(self, id: int, location: Point2D) -> None:
        if self.bounds is None:
            bounds_loc = Point2D(location.x, location.y)
            self.bounds = Rectangle2D(bounds_loc, 0, 0)
        else:
            self.bounds.encapsulate(location)
        vert = Vertex(id, location)
        self.vertices[id] = vert

    def add_edge(self, id: int, vert_id1: int, vert_id2: int, weight: int) -> None:
        vert1 = self.get_vertex(vert_id1)
        vert2 = self.get_vertex(vert_id2)
        edge = Edge(id, vert1, vert2, weight)
        self.edges[id] = edge
        vert1.add_edge(edge)
        vert2.add_edge(edge)

    def get_vertices(self) -> Iterable[Vertex]:
        return self.vertices.values()

    def get_vertex(self, vertId: int) -> Vertex:
        return self.vertices[vertId]

    def get_edges(self) -> Iterable[Edge]:
        return self.edges.values()

    def get_edge(self, edgeId: int) -> Edge:
        return self.edges[edgeId]

    def get_bounds(self) -> Rectangle2D:
        return self.bounds
