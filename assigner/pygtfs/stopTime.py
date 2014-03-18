'''
Created on Sep 2, 2013

@author: jacob
'''

class StopTime(object):

    def __init__(self, tripId, stopId, stopSequence, arrTimeMillis, depTimeMillis, postKm):
        '''
        
        tripId -- string representing GTFS trip.
        stopId -- string representing GTFS stop.
        stopSequence -- integer representing ordering of stops within a trip.
        arrTimeMillis -- arrival time, in milliseconds since 0000 local time.
        depTimeMillis -- departure time, in milliseconds since 0000 local time.
        '''
        self.tripId = tripId
        self.stopId = stopId
        self.stopSequence = stopSequence
        self.arrTimeMillis = arrTimeMillis
        self.depTimeMillis = depTimeMillis
        self.postKm = postKm
        
    def __eq__(self, other):
        return self.tripId == other.tripId and \
            self.stopId == other.stopId
            
    def __cmp__(self, other):
        if (self.stopSequence < other.stopSequence):
            return -1
        elif (self.stopSequence > other.stopSequence):
            return 1
        else:
            return 0
        

    def __repr__(self):
        return "StopTime(stop_id = %s, trip_id = %s)" % (self.stopId, self.tripId)
