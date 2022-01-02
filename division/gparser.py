from geometry import Point2D
from graph import Graph2D



class GraphParser:
    def __init__(self):
        self.graph = Graph2D()

    def parse_txt(self, filestream):
        vert_count = self.read_number(filestream)
        for i in range(vert_count):
            self.read_vertex(filestream.readline())
        edge_count = self.read_number(filestream)
        for i in range(edge_count):
            self.read_edge(filestream.readline())

    def parse_csv(self, vert_filestream, edge_filestream):
        for line in vert_filestream:
            self.read_vertex(line, ',')
        if edge_filestream is not None:
            for line in edge_filestream:
                self.read_edge(line, ',')

    def get_graph(self):
        return self.graph

    def read_number(self, filestream):
        txt = filestream.readline()
        return int(txt)

    def read_vertex(self, line, separator=' '):
        vert_data = line.split(separator)
        id = int(vert_data[0])
        x = int(vert_data[1])
        y = int(vert_data[2])
        self.graph.add_vertex(id, Point2D(x, y))

    def read_edge(self, line, separator=' '):
        edge_data = line.split(separator)
        vert_id1 = int(edge_data[0])
        vert_id2 = int(edge_data[1])
        weight = int(edge_data[2])
        id = int(edge_data[3])
        self.graph.add_edge(id, vert_id1, vert_id2, weight)

