import os


class RegionData:
    def __init__(self, rectangle, points):
        self.rectangle = rectangle
        self.points = points

    def __str__(self):
        s = '<RegionData rectangle:{} points:['.format(self.rectangle)
        for point in self.points:
            s += os.linesep + '\t' + str(point)
        s += ']>'
        return s


class NodeBase:
    def __init__(self, tree, rectangle):
        self.tree = tree
        self.rectangle = rectangle