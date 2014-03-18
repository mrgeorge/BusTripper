'''
Created on Sep 19, 2013

@author: jacob
'''

class AssignedTrip(object):
    '''
    classdocs
    '''


    def __init__(self, trip, block, initTime, initPost, stopSeqArr, stopSeqDep):
        '''
        Constructor
        '''
        self.trip = trip

        self.block = block
        
        self.time = initTime
        
        self._post = initPost
        self._postMax = initPost
        self.arrivedStop = stopSeqArr
        self.departedStop = stopSeqDep
        
        
        
    def getPost(self):
        return self._post
    def setPost(self, newPost):
        if newPost > self._post:
            self._postMax = newPost
        self._post = newPost
    def delPost(self):
        del self._post
    post = property(getPost, setPost, delPost, 
                    "Postmile along the given trip, in km.")
    
    
    def hasBacktracked(self):
        return self._postMax - self._post > 0.25
