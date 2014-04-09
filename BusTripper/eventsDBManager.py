import types
from collections import Iterable
import re
import numpy as np
import pandas as pd
import sqlite3

class EventsDB(object):
    """EventsDB class"""

    def __init__(self, dbFileLoc):
        self._dbFileLoc = dbFileLoc
        self.conn = sqlite3.connect(dbFileLoc)
        self.conn.row_factory = sqlite3.Row # to index rows by column names

        if self.getTableNames() == []:
            self.createDB()

        self.ensureIndices()

    def __del__(self):
        self.conn.commit()
        self.conn.close()

    def createDB(self):
        """Make a SQLite database with event_subset and raw_loc_subset tables

        Tables follow format of remote PostgreSQL db.
        """

        self.conn.executescript("""
        CREATE TABLE event_subset
        (
            device_id                TEXT     ,
            time                     INTEGER  ,
            type                     INTEGER  ,
            stop_id                  TEXT     ,
            stop_sequence            INTEGER  ,
            stop_index               INTEGER  ,
            stop_latitude            REAL     ,
            stop_longitude           REAL     ,
            stop_postmile            REAL     ,
            delay                    INTEGER  ,
            event_time               INTEGER  ,
            dwell_time_recommended   INTEGER  ,
            dwell_time_difference    INTEGER  ,
            driver_id                TEXT     ,
            bus_id                   TEXT     ,
            trip_id                  TEXT     ,
            route_id                 TEXT     ,
            dt                       INTEGER
        );

        CREATE TABLE raw_loc_subset
        (
            device_id                TEXT     ,
            time                     INTEGER  ,
            latitude                 REAL     ,
            longitude                REAL     ,
            speed                    REAL     ,
            bearing                  REAL     ,
            accuracy                 REAL     ,
            driver_id                TEXT     ,
            bus_id                   TEXT     ,
            dt                       INTEGER
        );
        """)

    def ensureIndices(self):
        """Create indices to speed up select queries"""
        self.conn.executescript("""
        CREATE INDEX IF NOT EXISTS idxEvDev ON event_subset(device_id);
        CREATE INDEX IF NOT EXISTS idxEvTime ON event_subset(time);
        CREATE INDEX IF NOT EXISTS idxEvTrip ON event_subset(trip_id);
        CREATE INDEX IF NOT EXISTS idxEvRoute ON event_subset(route_id);
        CREATE INDEX IF NOT EXISTS idxRLDev ON raw_loc_subset(device_id);
        CREATE INDEX IF NOT EXISTS idxRLTime ON raw_loc_subset(time);
        """
        )
        if "rlev" in self.getTableNames():
            self.conn.executescript("""
            CREATE INDEX IF NOT EXISTS idxRlevDev ON rlev(device_id);
            CREATE INDEX IF NOT EXISTS idxRlevTime ON rlev(time);
            CREATE INDEX IF NOT EXISTS idxRlevTrip ON rlev(trip_id);
            CREATE INDEX IF NOT EXISTS idxRlevRoute ON rlev(route_id);
            """
            )
        self.conn.commit()

    def getTableNames(self):
        cur = self.conn.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
        """)
        result = cur.fetchall()
        return [line[0] for line in result]

    def getColumnNames(self, tableName):
        if tableName not in self.getTableNames():
            raise ValueError(tableName)

        cur = self.conn.execute("PRAGMA TABLE_INFO({})".format(tableName))
        result = cur.fetchall()
        return [line[1] for line in result]

    def getDevicesForDate(self, date):
        """Get list of device_id strings for date"""
        return self.selectData(distinct=True, cols="device_id",
            tableName="raw_loc_subset", date=date)

    def getDatesForDevice(self, deviceID):
        """Get list of dates for deviceID"""
        cur = self.conn.execute("""
            SELECT DISTINCT DATE(time/1000,'UNIXEPOCH')
            FROM raw_loc_subset
            WHERE device_id = ?
        """, (deviceID,))
        return [row[0] for row in cur.fetchall()]
    
    def getTripsForDeviceDate(self, deviceID, date):
        """Get list of trip_id strings for device and date"""
        return self.selectData(distinct=True, cols="trip_id",
            tableName="event_subset", deviceID=deviceID, date=date)

    def selectData(self, distinct=False, cols=None, tableName="raw_loc_subset",
                   date=None, time=None, deviceID=None, tripID=None,
                   routeID=None, limit=None):
        """Construct and execute a general SELECT query

        SELECT [DISTINCT] {cols}
        FROM {tableName}
        [WHERE {date, time, deviceID, tripID, routeID}]
        """

        if distinct:
            qDist = "DISTINCT"
        else:
            qDist = ""
        
        # Get string of selected column names for query
        columns = self.getColumnNames(tableName)  # full list to check against

        if (cols is None) or (cols == '*'):  # no restriction on cols, get all
            qCols = "*"
            cols = columns
        elif (cols in columns):  # single string
            qCols = cols
            cols = (cols,)
        elif isinstance(cols, Iterable):  # list of columns
            qCols = []
            for col in cols:
                if col in columns:
                    qCols.append(col)
                else:
                    raise ValueError(col)
            qCols = ','.join(qCols)
        else:
            raise ValueError(col)


        # Get selection (where clauses)
        qWhere = []
        dateFmt = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$" # regex for YYYY-MM-DD
        timeFmt = "^[0-9]{2}:[0-9]{2}:[0-9]{2}$" # regex for HH:MM:SS

        if date is not None:
            try:
                if re.match(dateFmt, date):
                    qWhere.append("""
                    DATE(time/1000, 'UNIXEPOCH') = '{}'
                    """.format(date))
            except TypeError:
                if isinstance(date, Iterable): # range
                    if (re.match(dateFmt, date[0]) and
                        re.match(dateFmt, date[1])):
                        qWhere.append("""
                        DATE(time/1000,'UNIXEPOCH') >= '{}'
                        """.format(date[0]))
                        qWhere.append("""
                        DATE(time/1000, 'UNIXEPOCH') <= '{}'
                        """.format(date[1]))
                    else:
                        raise ValueError(date)

        if time is not None:
            try:
                if re.match(timeFmt, time):
                    qWhere.append("""
                    TIME(time/1000, 'UNIXEPOCH') = '{}'
                    """.format(time))
            except TypeError:
                if isinstance(time, Iterable): # range
                    if (re.match(timeFmt, time[0]) and
                        re.match(timeFmt, time[1])):
                        qWhere.append("""
                        TIME(time/1000,'UNIXEPOCH') >= '{}'
                        """.format(time[0]))
                        qWhere.append("""
                        TIME(time/1000, 'UNIXEPOCH') <= '{}'
                        """.format(time[1]))
                    else:
                        raise ValueError(time)

        if deviceID is not None:
            if isinstance(deviceID, types.StringTypes):
                qWhere.append("device_id = '{}'".format(deviceID))
            elif isinstance(deviceID, Iterable):
                qWhere.append("device_id IN {}".format(','.join(deviceID)))
            else:
                raise ValueError(deviceID)

        if tripID is not None:
            if isinstance(tripID, types.StringTypes):
                qWhere.append("trip_id = '{}'".format(tripID))
            elif isinstance(tripID, Iterable):
                qWhere.append("trip_id IN {}".format(','.join(tripID)))
            else:
                raise ValueError(tripID)

        if routeID is not None:
            if isinstance(routeID, types.StringTypes):
                qWhere.append("route_id = '{}'".format(routeID))
            elif isinstance(routeID, Iterable):
                qWhere.append("route_id IN {}".format(','.join(routeID)))
            else:
                raise ValueError(routeID)

        if len(qWhere) > 0:
            qWhere = "WHERE " + ' AND '.join(qWhere)

        # Limit clause
        if limit is None:
            qLim = ""
        else:
            qLim = "LIMIT {}".format(limit)

        # Combine clauses into query
        query = "SELECT {} {} FROM {} {} {}".format(qDist, qCols, tableName,
                                                    qWhere, qLim)
        # Execute query and return pandas DataFrame
        df = pd.io.sql.frame_query(query, self.conn)

        # Convert time in unix ms to datetime object and set timezone
        if "time" in df.columns:
            df["time"] = df["time"].apply(pd.datetools.to_datetime,unit='ms')
            df["time"] = df["time"].apply(lambda x: x.tz_localize("UTC").tz_convert("Europe/Madrid"))

        return df

    def selectDataPrompt(self):
        """Prompt user for args to selectData"""
        cols = raw_input('Enter a column name: ')
        if cols.lower() == "none":
            cols = None
        tableName="raw_loc_subset"
        date = raw_input('Enter a date: ') #YYYY-MM-DD
        if date.lower=="none":
            date = None
        time = raw_input('Enter a time: ') #HH:MM:SS
        if time.lower == "none":
            time = None
        deviceID = raw_input('Enter a deviceID: ')
        if deviceID.lower == "none":
            deviceID = None
        limit = raw_input('Enter a limit: ')
        return selectData(cur, cols, tableName, date, time, deviceID, int(limit))

    def joinRawToEvents(self):
        """Create a table that joins raw location with events table

        First group events by device_id, date, and trip_id.
        Then match all the raw location entries to the device/day/trip groups
            based on the time interval.
        The created view can be used as a training set with raw locations and
            the "ground truth" trip/route assignment.

        Note: This join is very slow with python sqlite.
              Try with command line interface instead.
        """
        query = """
        CREATE TABLE IF NOT EXISTS rlev AS
        SELECT eg.trip_id, eg.route_id, r.device_id,
            r.time, r.latitude, r.longitude,
            r.speed, r.bearing, r.accuracy
        FROM raw_loc_subset AS r
        LEFT OUTER JOIN
            (SELECT device_id, DATE(time/1000,'unixepoch') AS dd,
                 MIN(time) AS tmin, MAX(time) AS tmax, trip_id, route_id
             FROM event_subset
             GROUP BY device_id,dd,trip_id) AS eg
        ON (eg.device_id = r.device_id AND
            eg.dd = DATE(r.time/1000,'unixepoch') AND
            r.time BETWEEN tmin AND tmax);
        """
        self.conn.execute(query)
        self.conn.commit()



def rowsToRec(rows, cols):
    """DEPRECATED - now using pandas dataframes instead.

    Translate SQLite rows array into numpy recarray"""
    data = [np.array([row[str(col)] for row in rows]) for col in cols]
    fmt = [arr.dtype.str for arr in data]
    rec = np.recarray(data[0].size, formats = fmt, names = cols)
    for colNum,col in enumerate(cols):
        rec[col] = data[colNum]

    return rec
