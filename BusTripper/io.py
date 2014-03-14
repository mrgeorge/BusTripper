import types
from collections import Iterable
import re
import numpy as np
import sqlite3

try:
    import psycopg2
    hasPsycopg=True
except:
    hasPsycopg=True


def readKeys(filename):
    """Construct dict with keys from file

    Expects text file with 1 key per line in the format
    key=value
    Lines can be commented by starting with '#'
    """
    loginDict = {}
    comments = ('#')
    with open(filename) as ff:
        for line in ff:
            if line[0] not in comments:
                elements = line.rstrip().split("=")
                loginDict[elements[0]] = elements[1]

    return loginDict

def createLocalDB(localDBFilename):
    """Make a local sqlite database with same format as remote PostgreSQL"""

    lcon, lcur = openLocalDB(localDBFilename)
    lcur.executescript("""
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
    lcur.close()
    lcon.close()

def copyRemoteRows(lcur, rcur, limit = 10):
    """Copy entries from remote db to local db

    Inputs:
        lcur - sqlite cursor for local db
        rcur - psycopg2 cursor for remote db
        limit - number of rows to copy (None to copy all rows)
    """

    if limit is None:
        limStr = ""
    else:
        limStr = " LIMIT {}".format(limit)

    for table in ("event_subset", "raw_loc_subset"):
        rcur.execute("SELECT * FROM " + table + limStr)
        colNames = [col.name for col in rcur.description]
        nCols = len(colNames)
        colStr = '(' + ','.join(colNames) + ')'
        valStr = '(' + ('?,' * nCols)[:-1] + ')'
        lcur.executemany("INSERT INTO " + table + colStr +
                         " VALUES " + valStr, rcur)

def copyRemoteDB(loginFilename, localDBFilename, limit = 10):
    """Create and fill a local sqlite DB from remote Postgres

    Should only need to run this once per machine.

    Inputs:
        loginFilename - file with keys for remote db
        localDBFilename - name for new sqlite db
        limit - number of rows to copy (None to copy all rows)
    """

    # Make local DB
    createLocalDB(localDBFilename)
    lcon, lcur = openLocalDB(localDBFilename)

    # Open remote DB
    if not hasPsycopg:
        raise ImportError(psycopg2)
    loginDict = readKeys(loginFilename)
    rcon = psycopg2.connect(**loginDict)
    rcur = rcon.cursor()

    # Copy remote to local
    copyRemoteRows(lcur, rcur, limit=limit)

    # Save changes and close
    lcon.commit()
    [xx.close() for xx in lcur, lcon, rcur, rcon]

def openLocalDB(localDBFilename):
    """Open connection and cursor to SQLite database"""
    con = sqlite3.connect(localDBFilename)
    con.row_factory = sqlite3.Row # allows indexing rows by column names
    cur = con.cursor()
    return (con, cur)

def getTableNames(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    result = cur.fetchall()
    return [line[0] for line in result]

def getColumnNames(cur, tableName):
    if tableName not in getTableNames(cur):
        raise ValueError(tableName)

    cur.execute("PRAGMA TABLE_INFO({})".format(tableName))
    result = cur.fetchall()
    return [line[1] for line in result]

def selectData(cur, cols=None, tableName="raw_loc_subset", date=None,
               time=None, deviceID=None, limit=None):
    """Construct and execute select query

    SELECT {cols} FROM {tableName} [WHERE {date, time, deviceID}]
    """

    # Get string of selected column names for query
    columns = getColumnNames(cur, tableName)  # full list to check against

    if cols is None:  # no restriction on cols, select all
        qCols = "*"
        cols = columns
    elif (cols in columns) or (cols == '*'):  # single string
        qCols = cols
        cols = columns
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
        if re.match(dateFmt, date):
            qWhere.append("DATE(time/1000, 'UNIXEPOCH') = '{}'".format(date))
        elif isinstance(date, Iterable): # range
            if re.match(dateFmt, date[0]) and re.match(dateFmt, date[1]):
                qWhere.append("DATE(time/1000,'UNIXEPOCH') >= '{}'".format(
                    date[0]))
                qWhere.append("DATE(time/1000, 'UNIXEPOCH') <= '{}'".format(
                    date[1]))
            else:
                raise ValueError(date)
        else:
            raise ValueError(date)

    if time is not None:
        if re.match(timeFmt, time):
            qWhere.append("TIME(time/1000, 'UNIXEPOCH') = '{}'".format(time))
        elif isinstance(time, Iterable): # range
            if re.match(timeFmt, time[0]) and re.match(timeFmt, time[1]):
                qWhere.append("TIME(time/1000,'UNIXEPOCH') >= '{}'".format(
                    time[0]))
                qWhere.append("TIME(time/1000, 'UNIXEPOCH') <= '{}'".format(
                    time[1]))
            else:
                raise ValueError(time)
        else:
            raise ValueError(time)

    if deviceID is not None:
        if isinstance(deviceID, types.StringTypes):
            qWhere.append("device_id = '{}'".format(deviceID))
        elif isinstance(deviceID, Iterable):
            qWhere.append("device_id IN {}".format(','.join(deviceID)))
        else:
            raise ValueError(deviceID)

    if len(qWhere) > 0:
        qWhere = "WHERE " + ' AND '.join(qWhere)

    # Limit clause
    if limit is None:
        qLim = ""
    else:
        qLim = "LIMIT {}".format(limit)

    # Combine clauses and execute query
    query = "SELECT {} FROM {} {} {}".format(qCols, tableName, qWhere, qLim)
    cur.execute(query)
    result = cur.fetchall()

    # Translate into numpy recarray
    data = [np.array([row[str(col)] for row in result]) for col in cols]
    fmt = [arr.dtype.str for arr in data]
    rec = np.recarray(data[0].size, formats = fmt, names = cols)
    for colNum,col in enumerate(cols):
        rec[col] = data[colNum]

    return rec

def selectDevices(cur, date):
    cur.execute("""SELECT DISTINCT device_id
                FROM raw_loc_subset
                WHERE DATE(time/1000.,'UNIXEPOCH') = '%s'
                """ % date)
    return [row[0] for row in cur.fetchall()]

def custom(localDBFilename):
    cur = openLocalDB(localDBFilename)[1]
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
