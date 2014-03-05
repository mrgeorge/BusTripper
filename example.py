import psycopg2


def readLogin(filename):
    """Construct dict with login keys from file

    Expects file with 1 key per line in the format
    key=value
    """
    loginDict = {}
    with open(filename) as ff:
        for line in ff:
            elements = line.rstrip().split("=")
            loginDict[elements[0]] = elements[1]

    return loginDict

if __name__ == "__main__":

    loginFile = "remotedb.login"
    loginDict = readLogin(loginFile)
    
    conn = psycopg2.connect(**loginDict)
    cur = conn.cursor()

    exec_str = "SELECT * FROM event_subset LIMIT 10"

    cur.execute(exec_str)
    for rec in cur:
        print rec

    cur.close()
    conn.close()
