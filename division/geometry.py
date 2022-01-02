class Point2D:
    def __init__(self, x=0 , y=0):
        self.x = x
        self.y = y

    def __str__(self):
        return '<Point2D x:{} y:{}>'.format(self.x, self.y)


class Rectangle2D:
    def __init__(self, location: Point2D=Point2D(), width=0, height=0):
        self.location = location
        self.width = width
        self.height = height

    def __str__(self):
        return '<Rectangle2D location:{} width:{} height:{}>'.format(self.location, self.width, self.height)

    def left(self):
        return self.location.x

    def bottom(self):
        return self.location.y

    def right(self):
        return self.left() + self.width

    def top(self):
        return self.bottom() + self.height

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
