from typing import Iterable


class Point2D:
    def __init__(self, x: int=0, y: int=0):
        self.x = x
        self.y = y

    def __str__(self) -> str:
        return f'<Point2D x:{self.x} y:{self.y}>'


class Rectangle2D:
    def __init__(self, location: Point2D=Point2D(), width: int=0, height: int=0):
        self.location = location
        self.width = width
        self.height = height

    def __str__(self) -> str:
        return f'<Rectangle2D location:{self.location} width:{self.width} height:{self.height}>'

    def left(self) -> int:
        return self.location.x

    def bottom(self) -> int:
        return self.location.y

    def right(self) -> int:
        return self.left() + self.width

    def top(self) -> int:
        return self.bottom() + self.height

    def center(self) -> Point2D:
        x = self.left() + self.width // 2
        y = self.bottom() + self.height // 2
        return Point2D(x, y)

    def encapsulate(self, point: Point2D) -> None:
        if self.left() > point.x:
            self.width += self.left() - point.x
            self.location.x = point.x
        elif point.x >= self.right():
            self.width += point.x - self.right() + 1
        if self.bottom() > point.y:
            self.height += self.bottom() - point.y
            self.location.y = point.y
        elif point.y >= self.top():
            self.height += point.y - self.top() + 1

    def contains(self, point: Point2D) -> bool:
        return point.x >= self.left() and point.x < self.right() and point.y >= self.bottom() and point.y < self.top()

    def encapsulate_all(points: Iterable[Point2D]) -> 'Rectangle2D':
        bounds: Rectangle2D = None
        for point in points:
            if bounds is None:
                bounds = Rectangle2D(point, 0, 0)
            else:
                bounds.encapsulate(point)
        return bounds
