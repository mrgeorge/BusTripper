import psycopg2
import BusTripper

if __name__ == "__main__":

    loginFile = "data/remotedb.login"
    loginDict = BusTripper.io.readKeys(loginFile)

    conn = psycopg2.connect(**loginDict)
    cur = conn.cursor()

    exec_str = "SELECT * FROM event_subset LIMIT 10"

    cur.execute(exec_str)
    for rec in cur:
        print rec

    cur.close()
    conn.close()
