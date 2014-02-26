class Agent(object):
    def __init__(self, id, x, y, ex, ey):
        self.id = id
        self.x = x
        self.y = y
        self.ex = ex
        self.ey = ey

    def updatePosition(self, x, y, ex, ey):
        self.x = x
        self.y = y
        self.ex = ex
        self.ey = ey

class Node(object):
    def __init__(self, x, y, neighbors=None):
        self.x = x
        self.y = y
        self.neighbors = neighbors

class Map(object):
    def __init__(self, nodes):
        self.nodes = nodes

class Simulator(object):
    def __init__(self, nSteps):
        self.nSteps = nSteps
