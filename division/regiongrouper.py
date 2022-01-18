import sys
from itertools import cycle
from matplotlib import pyplot as plt

from geometry import Point2D, Rectangle2D
from gparser import GraphParser
from graph import Graph2D
from kdtree import KdTree



class RegionCenter:
    def __init__(self, region_id: int, location: Point2D):
        self.region_id = region_id
        self.location = location

def plot_points(pointsCollection, color):
    x_coordinates = []
    y_coordinates = []

    for point in pointsCollection:
        x_coordinates.append(point.x)
        y_coordinates.append(point.y)
    plt.plot(x_coordinates, y_coordinates, 'o', color=color)


region_filename = sys.argv[1]
verts_filename = sys.argv[2]
max_accumulation = int(sys.argv[3])
region_assignment_filename = sys.argv[4]

graph: Graph2D = None
with open(verts_filename) as verts_filestream:
    parser = GraphParser()
    parser.parse_csv(verts_filestream, None)

    graph = parser.get_graph()

region_bounds = dict[int, Rectangle2D]()
with open(region_filename) as region_filestream:
    region_data = region_filestream.readlines()
    region_count = len(region_data) // 2
    for i in range(region_count):
        region_id = int(region_data[i * 2])
        vert_ids = [int(vert_id) for vert_id in region_data[i * 2 + 1].split()]
        vert_pos = [graph.get_vertex(vert_id).location for vert_id in vert_ids]
        region_bounds[region_id] = Rectangle2D.encapsulate_all(vert_pos)

region_centers = [RegionCenter(region_id, bounds.center()) for (region_id, bounds) in region_bounds.items()]
region_centers_bounds = Rectangle2D.encapsulate_all([r.location for r in region_centers])

tree = KdTree(region_centers_bounds, max_accumulation)
for r in region_centers:
    tree.add(r)
tree.divide()

colors = cycle("bgrcmyk")

region_regions = tree.get_regions()
with open(region_assignment_filename, 'x') as region_assignment_filestream:
    for r in region_regions:
        region_data = [str(rc.region_id) for rc in r.items]
        region_data_str = str.join(" ", region_data)
        region_assignment_filestream.write(region_data_str + '\n')

        plot_points([v.location for v in r.items], next(colors))

plt.show()
