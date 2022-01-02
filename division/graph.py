import os

from geometry import Point2D, Rectangle2D



class Vertex:
    def __init__(self, id, location):
        self.id = id
        self.location = location
        self.edges = dict()

    def __str__(self):
        return "<Vertex id:{} location:{}>".format(self.id, self.location)

    def add_edge(self, edge):
        self.edges[edge.id] = edge

    def get_edges(self):
        return self.edges.values()


class Edge:
    def __init__(self, id, vert1, vert2, weight):
        self.id = id
        self.vert1 = vert1
        self.vert2 = vert2
        self.weight = weight
        
    def __str__(self):
        return "<Edge id:{} vert1:{} vert2:{} weight:{}>".format(self.id, self.vert1, self.vert2, self.weight)


class Graph2D:
    def __init__(self):
        self.vertices = dict()
        self.edges = dict()
        self.bounds = None

    def __str__(self):
        vertStr = ""
        for vert in self.vertices.values():
            vertStr += str(vert) + os.linesep + "\t"
        edgeStr = ""
        for edge in self.edges.values():
            edgeStr += str(edge) + os.linesep + "\t"
        return "<Graph2D bounds:{} vertices:[{}] edges:[{}]".format(self.bounds, vertStr, edgeStr)

    def add_vertex(self, id, location):
        if self.bounds is None:
            bounds_loc = Point2D(location.x, location.y)
            self.bounds = Rectangle2D(bounds_loc, 0, 0)
        else:
            self.bounds.encapsulate(location)
        vert = Vertex(id, location)
        self.vertices[id] = vert

    def add_edge(self, id, vert_id1, vert_id2, weight):
        vert1 = self.get_vertex(vert_id1)
        vert2 = self.get_vertex(vert_id2)
        edge = Edge(id, vert1, vert2, weight)
        self.edges[id] = edge
        vert1.add_edge(edge)
        vert2.add_edge(edge)

    def get_vertices(self):
        return self.vertices.values()

    def get_vertex(self, vertId):
        return self.vertices[vertId]

    def get_edges(self):
        return self.edges.values()

    def get_edge(self, edgeId):
        return self.edges[edgeId]

    def get_bounds(self):
        return self.bounds
