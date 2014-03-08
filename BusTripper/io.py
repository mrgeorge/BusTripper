import psycopg2
import sqlite3

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

    lcon = sqlite3.connect(localDBFilename)
    lcur = lcon.cursor()
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
    """Copy enties from remote db to local db

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
    lcon = sqlite3.connect(localDBFilename)
    lcur = lcon.cursor()

    # Open remote DB
    loginDict = readKeys(loginFilename)
    rcon = psycopg2.connect(**loginDict)
    rcur = rcon.cursor()

    # Copy remote to local
    copyRemoteRows(lcur, rcur, limit=limit)

    # Save changes and close
    lcon.commit()
    [xx.close() for xx in lcur, lcon, rcur, rcon]
