'''
Created on Dec 4, 2013

@author: jacob
'''

class Service(object):
    '''
    classdocs
    '''


    def __init__(self, serviceId, startDate, endDate, dayList):
        '''
        Constructor
        '''
        self.serviceId = serviceId
        
        # dates in text format YYYYMMDD 
        self.startDate = startDate
        self.endDate = endDate
        
        # list of seven booleans representing whether this service runs
        # on a given day of the week (Monday = 0, Tuesday = 1, etc.)
        self.dayList = dayList
        
        
    def __eq__(self, other):
        return self.serviceId == other.serviceId
    
    
    def __str__(self, *args, **kwargs):
        return "Service(service_id=%s)" % (self.serviceId)