
from geometry import Point2D, Rectangle2D
from tree import RegionData, NodeBase


class KdNode(NodeBase):
    def __init__(self, tree, rectangle, node1_rectangle, node2_rectangle):
        NodeBase.__init__(self, tree, rectangle)
        self.node1 = AccumulatingNode(tree, node1_rectangle)
        self.node2 = AccumulatingNode(tree, node2_rectangle)

    def add(self, item):
        if self.node1.rectangle.contains(item.location):
            self.node1.add(item)
        else:
            self.node2.add(item)

    def divide(self, depth):
        self.node1 = self.node1.divide(depth + 1)
        self.node2 = self.node2.divide(depth + 1)
        return self

    def get_regions(self):
        regions = []
        regions.extend(self.node1.get_regions())
        regions.extend(self.node2.get_regions())
        return regions


class AccumulatingNode(NodeBase):
    def __init__(self, tree, rectangle):
        NodeBase.__init__(self, tree, rectangle)
        self.items = list()

    def add(self, item):
        self.items.append(item)

    def divide(self, depth):
        if len(self.items) <= self.tree.max_accumulation:
            return self
        else:
            axis = depth % 2
            if axis == 0:
                new_node = self.divide_vertical()
            else:
                new_node = self.divide_horizontal()
            for item in self.items:
                new_node.add(item)
            new_node = new_node.divide(depth)
            return new_node

    def get_regions(self):
        return [ RegionData(self.rectangle, self.items) ]

    def divide_vertical(self):
        rect = self.rectangle
        sorted_items = sorted(self.items, key=lambda x: x.location.x)
        sorted_len = len(sorted_items)
        median_index = min(sorted_len - 1, (sorted_len // 2) + 1)
        median_item = sorted_items[median_index]
        rect1 = Rectangle2D(rect.location, median_item.location.x - rect.left(), rect.height)
        rect2 = Rectangle2D(Point2D(rect1.right(), rect.bottom()), rect.right() - rect1.right(), rect.height)
        return KdNode(self.tree, self.rectangle, rect1, rect2)

    def divide_horizontal(self):
        rect = self.rectangle
        sorted_items = sorted(self.items, key=lambda x: x.location.y)
        sorted_len = len(sorted_items)
        median_index = min(sorted_len - 1, (sorted_len // 2) + 1)
        median_item = sorted_items[median_index]
        rect1 = Rectangle2D(rect.location, rect.width, median_item.location.y - rect.bottom())
        rect2 = Rectangle2D(Point2D(rect.left(), rect1.top()), rect.width, rect.top() - rect1.top())
        return KdNode(self.tree, self.rectangle, rect1, rect2)


class KdTree:
    def __init__(self, rectangle, max_accumulation):
        self.main_node = AccumulatingNode(self, rectangle)
        self.max_accumulation = max_accumulation

    def add(self, item):
        self.main_node.add(item)

    def divide(self):
        self.main_node = self.main_node.divide(0)

    def get_regions(self):
        return self.main_node.get_regions()
