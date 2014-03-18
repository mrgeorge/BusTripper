'''
Created on Sep 8, 2013

@author: jacob
'''
from pygtfs.util import kmBetweenLatLonPair

class Shape(object):
    '''
    classdocs
    '''


    def __init__(self, shapeId):
        '''
        Constructor
        '''
        self.shapeId = shapeId
        self.pointList = []
        
        
    def __eq__(self, other):
        return self.shapeId == other.shapeId
    
    
    def addPoint(self, lat, lon, seq):
        post = 0.0
        if (len(self.pointList) > 0):
            prevLat = self.pointList[-1]['lat']
            prevLon = self.pointList[-1]['lon']
            dist = kmBetweenLatLonPair(lat, lon, prevLat, prevLon)
            post = self.pointList[-1]['post'] + dist
            
        point = {'lat' : lat, 'lon' : lon, 
                 'seq' : seq, 'post' : post}
        self.pointList.append(point)