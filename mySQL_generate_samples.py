import _mysql


def main():
    conn = _mysql.connect(host='127.0.0.1', user='root')

    create_schema(conn)

    queries_insert(conn, n=10)

    queries_select(conn)

def create_schema(conn):
    conn.query('DROP DATABASE IF EXISTS mySQL_database')
    conn.query('CREATE DATABASE mySQL_database')
    conn.select_db('mySQL_database')
    conn.query('''
        CREATE TABLE SCANS
        (
           SCAN_ID                BIGINT NOT NULL,
           SCAN_HASH              VARCHAR(11) NOT NULL,
           SCAN_TYPE              VARCHAR(3),
           SCAN_COUNT             INT,
           MACHINE_TYPE           VARCHAR(10),
           SEQUENCE_CODE          VARCHAR(5),
           LOAD_DATE              TIMESTAMP,
           PRIMARY KEY (SCAN_ID)
        )
        ''')

def queries_insert(conn, n):

    for i in xrange(n):
        fields = []
        SCAN_ID = ['fasdfad']
        SCAN_HASH = [str(i)]
        SCAN_TYPE = ['fda']
        SCAN_COUNT = [ '8']
        MACHINE_TYPE = ['dfadfa']
        SEQUENCE_CODE = ['dfadfa']
        LOAD_DATE = ['2013-02-21']
        fields = SCAN_ID + SCAN_HASH + SCAN_TYPE + SCAN_COUNT + MACHINE_TYPE \
            + SEQUENCE_CODE + LOAD_DATE
        conn.query('INSERT INTO SCANS VALUES (%s)' % ','.join(fields))


def queries_select(conn):
    conn.query('SELECT * FROM SCANS')
    result = conn.use_result()

    row = result.fetch_row()
    while row:
        print row
        row = result.fetch_row()


if __name__ == '__main__':
    main()