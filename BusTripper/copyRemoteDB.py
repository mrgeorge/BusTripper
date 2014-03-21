import psycopg2
import eventsDBManager

"""Functions to download remote PostgreSQL db and copy to local SQLite db"""
    
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
    localDB = eventsDBManager.EventsDB(localDBFilename)
    lcur = localDB.conn.cursor()
    
    # Open remote DB
    loginDict = readKeys(loginFilename)
    rcon = psycopg2.connect(**loginDict)
    rcur = rcon.cursor()

    # Copy remote to local
    copyRemoteRows(lcur, rcur, limit=limit)

    # Save changes and close
    localDB.conn.commit()
    [xx.close() for xx in lcur, localDB.conn, rcur, rcon]
