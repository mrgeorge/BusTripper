'''
Created on Sep 8, 2013

@author: jacob
'''

class Stop(object):
    '''
    classdocs
    '''


    def __init__(self, stopId, stopLat, stopLon, stopName=None):
        '''
        Constructor
        '''
        self.stopId, self.stopLat, self.stopLon = stopId, stopLat, stopLon
        self.stopName = stopName

    def __eq__(self, other):
        return self.stopId == other.stopId


    def __repr__(self):
        return "Stop(%s)" % (self.stopId)
