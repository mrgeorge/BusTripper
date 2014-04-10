'''
Created on Sep 2, 2013

@author: jacob
'''

from pygtfs.gtfsData import GtfsData
from argparse import ArgumentParser
import logging
import time
import os
from tripClassifier import TripClassifier

class Predictor(object):
    '''
    classdocs
    '''


    def __init__(self, agency, loggerName, dbFileLoc):
        '''
        Constructor
        '''
        self.gtfsData = GtfsData(agency, dbFileLoc)
            
        self.tripClassifier = TripClassifier(self.gtfsData, loggerName)
        
        self.logger = logging.getLogger(loggerName)
        
        self.time = 0
        
        self.count = 0
        
        # try to assign unassigned trips once per 30 seconds
        self.prevAssignment = 0
        self.assignDt = 60*1000
        
        
    def newRawLocation(self, rawLocation):
        self.count += 1
        self.logger.debug("Received message %d." % self.count)
        if (self.count % 1000 == 0):
            self.logger.info("Received message %d." % self.count)
        self.tripClassifier.newRawLocation(rawLocation)
        newTime = rawLocation.ts
        
        # update the internal clock
        self.updateTime(newTime)
        
                
    def updateTime(self, newTime):
        self.time = max(self.time, newTime)
        self.tripClassifier.updateTime(self.time)
        
        if (self.time - self.prevAssignment > self.assignDt):
            timeString = time.strftime("%D %H:%M:%S", time.localtime(self.time/1000.))
            self.logger.info(timeString)
            self.prevAssignment = self.time
            self.tripClassifier.checkAllUnassignedForObviousBlocks()
            # push list of assigned trips to Mosquitto!
        

if __name__ == "__main__":
    parser = ArgumentParser(prog='Predictor')
    parser.add_argument("-a", "--agency-name", dest="agency", 
                        metavar="AGENCY",
                        required=True,
                        help="name of agency")
    parser.add_argument("-l", "--log-file", dest="logfile", 
                        metavar="LOGFILE",
                        default=None,
                        help="file for log messages")
    parser.add_argument("-L", "--log-level", dest="loglevel", 
                        metavar="LOGLEVEL",
                        default="WARNING",
                        help="log level of Predictor (one of CRITICAL, ERROR, WARNING, INFO, or DEBUG)",
                        choices=('CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'))
    parser.add_argument("-g", "--gtfs-db-file", dest="dbfileloc", 
                        required=True,
                        metavar="GTFS_DB_FILE",
                        help="path to GTFS data (in sqlite3 db format)")
    
    args = parser.parse_args()
        
    loggerName = '%s_assigner' % (args.agency)
    
    logger = logging.getLogger(loggerName)

    logLevel = getattr(logging, args.loglevel)
    logfile = args.logfile

    if (logfile is not None):
        logging.basicConfig(format='%(asctime)s [%(module)s.%(funcName)s] %(message)s',
                            filename=logfile,
                            level=logLevel)
    else:
        logging.basicConfig(format='%(asctime)s [%(module)s.%(funcName)s] %(message)s',
                            level=logLevel)
        
    myPredictor = Predictor(args.agency, loggerName, args.dbfileloc)

    ## LocationManager CLASS NEEDS TO BE IMPLEMENTED ##
    # Basically, all it needs to call the newRawLocation method of myPredictor
    # whenever there's a new raw location you want to give it.
    
    locMan = LocationManager(myPredictor)
    
    tStart = time.time()
    
    tLim = 60*60*24*365*10
    
    prevTime = 0

    # I had implemented the listener as a Thread, as you can see here. This
    # is because the messaging solution I was using was event-based and I was
    # explicitly handling the threading. There's no real need for you to do
    # this for your project.
    # locMan.start()
    # while True:
    #     time.sleep(0.01)
    #     currTime = int(time.time() - tStart)
    #     if (currTime > prevTime):
    #         prevTime = currTime
    #     if (currTime > tStart + tLim):
    #         locMan.join()
    #         break

    #Pseudocode: for each raw location in the events DB, update
