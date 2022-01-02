import sys
from matplotlib import pyplot as plt
from itertools import cycle

from kdtree import KdTree
from gparser import GraphParser



def plot_points(pointsCollection, color):
    x_coordinates = []
    y_coordinates = []

    for point in pointsCollection:
        x_coordinates.append(point.x)
        y_coordinates.append(point.y)
    plt.plot(x_coordinates, y_coordinates, 'o', color=color)


vert_filename = sys.argv[1]
max_accumulation = int(sys.argv[2])

with open(vert_filename) as vert_filestream:
    parser = GraphParser()
    parser.parse_csv(vert_filestream, None)

    graph = parser.get_graph()

    tree = KdTree(graph.get_bounds(), max_accumulation)
    for vert in graph.get_vertices():
        tree.add(vert)
    tree.divide()
        
    colors = cycle("bgrcmyk")
    
    regions = tree.get_regions()
    for i in range(len(regions)):
        region = regions[i]
        plot_points([v.location for v in region.points], next(colors))

    plt.show()