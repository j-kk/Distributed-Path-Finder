import os
from typing import Iterable

from geometry import Rectangle2D


class RegionData:
    def __init__(self, rectangle: Rectangle2D, items: Iterable):
        self.rectangle = rectangle
        self.items = items

    def __str__(self) -> str:
        s = '<RegionData rectangle:{} points:['.format(self.rectangle)
        for point in self.items:
            s += os.linesep + '\t' + str(point)
        s += ']>'
        return s


class NodeBase:
    def __init__(self, tree, rectangle: Rectangle2D):
        self.tree = tree
        self.rectangle = rectangle