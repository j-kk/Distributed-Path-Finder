import sys
from matplotlib import pyplot as plt
from itertools import cycle

from gparser import GraphParser
from kdtree import KdTree
from regionconsolidator import consolidate_regions



def plot_points(pointsCollection, color):
    x_coordinates = []
    y_coordinates = []

    for point in pointsCollection:
        x_coordinates.append(point.x)
        y_coordinates.append(point.y)
    plt.plot(x_coordinates, y_coordinates, 'o', color=color)


vert_filename = sys.argv[1]
edge_filename = sys.argv[2]
max_accumulation = int(sys.argv[3])

with open(vert_filename) as vert_filestream, open(edge_filename) as edge_filestream:
    parser = GraphParser()
    parser.parse_csv(vert_filestream, edge_filestream)

    graph = parser.get_graph()

    tree = KdTree(graph.get_bounds(), max_accumulation)
    for vert in graph.get_vertices():
        tree.add(vert)
    tree.divide()
        
    colors = cycle("bgrcmyk")
    
    regions = tree.get_regions()
    regionVerts = [r.items for r in regions]
    regions2 = consolidate_regions(regionVerts)

    for i in range(len(regions2)):
        region = regions2[i]
        plot_points([v.location for v in region], next(colors))

    plt.show()