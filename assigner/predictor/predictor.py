'''
Created on Sep 2, 2013

@author: jacob
'''

from pygtfs.gtfsData import GtfsData
from argparse import ArgumentParser
import logging
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from tripClassifier import TripClassifier
import sqlite3
from rawLocation import rawLocation
import pickle as pickle

import os, sys
try:
    import BusTripper
except ImportError: # add grandparent dir to python search path
    path, filename = os.path.split(__file__)
    sys.path.append(os.path.abspath(os.path.join(path,"../../")))
    import BusTripper


class Predictor(object):
    '''
    classdocs
    '''


    def __init__(self, agency, loggerName, dbFileLoc):
        '''
        Constructor
        '''
        
        self.logger = logging.getLogger(loggerName)
        self.logger.info("Attempting to load GtfsData from pickle.")
        try:
            with open('temp.pickle','r') as f:
                self.gtfsData = pickle.load(f)
        except:
            self.logger.info("Failed to load GtfsData from pickle. Loading from dbfile.")
            self.gtfsData = GtfsData(agency, dbFileLoc)
            with open('temp.pickle','wb') as f:
                pickle.dump(self.gtfsData, f)
            
        self.tripClassifier = TripClassifier(self.gtfsData, loggerName)
        
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
    parser.add_argument("-e", "-events-db-file", dest = "events_db",
                        required=True,
                        metavar="EVENTS_DB_FILE",
                        help="path to events data (in sqlite3 db format)")
    
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
    
    #locMan = LocationManager(myPredictor)
    
    tStart = time.time()
    
    # tLim = 60*60*24*365*10
    
    # prevTime = 0

    timeLimit = 3600 + tStart #cap at 1 hour



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
    #No need for location manager?  -- Just feed data from db directly into self.newRawLocation (not sure about this)

    dateStart = datetime(2013, 12, 1)
    dateStartStr = dateStart.date().isoformat()
    dateEndStr = (dateStart + relativedelta(days=0)).date().isoformat()
    #print "Selecting data from {} to {}".format(dateStartStr, dateEndStr)
    try:
        with open('rl.temp.pickle','r') as f:
            df = pickle.load(f)
    except:
        with open('rl.temp.pickle','wb') as f:
            db = BusTripper.eventsDBManager.EventsDB(args.events_db)
            df = db.selectData(cols=("device_id", "time", "latitude",
                                     "longitude"),
                               tableName="raw_loc_subset",
                               date=(dateStartStr, dateEndStr),
#                               time=("11:00:00", "11:05:00"),
                               convertTime=False)
            pickle.dump(df, f)

    print "Adding data for this month to predictor"
    for ind,row in df.iterrows():
        rawLoc = rawLocation(row['device_id'], row['time'],
                             row['latitude'], row['longitude'])
        #print rawLoc.ts
        myPredictor.newRawLocation(rawLoc)

        if (time.time() > timeLimit):
            print "Time is up - breaking..."
            myPredictor.tripClassifier.assignedTrips.getAccuracy() #breaking abstraction barriers, to be cleaned up
            break

    print "Cumulative accuracy through {}".format(dateEndStr)
    myPredictor.tripClassifier.assignedTrips.getAccuracy() #breaking abstraction barriers, to be cleaned up

    

    #for ii in range(nMonths):
