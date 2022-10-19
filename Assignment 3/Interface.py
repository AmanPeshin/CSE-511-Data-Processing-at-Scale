#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    c = openconnection.cursor()
    QUERY = "CREATE TABLE {t} (userid int not null, movieid int, rating float, timestamp int);"
    c.execute("DROP TABLE IF EXISTS " + ratingstablename)
    c.execute(QUERY.format(t=ratingstablename))
    with open(ratingsfilepath) as text:
        for item in text:
            line = item.split('::')
            c.execute('INSERT INTO {t} VALUES ({u_id}, {m_id}, {rtg}, {tmsp})'.format(t=ratingstablename, u_id=line[0], m_id=line[1], rtg=line[2], tmsp=line[3]))
    c.execute("ALTER TABLE " + ratingstablename + " DROP timestamp")
    c.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    highest_rating = 5.0
    c = openconnection.cursor()
    stepsize = highest_rating / numberofpartitions
    QUERY_0 = 'CREATE TABLE range_part{iter} AS SELECT * FROM {table} WHERE rating>={lower_limit} and rating<={higher_limit}'
    QUERY_1 = 'CREATE TABLE range_part{iter} AS SELECT * FROM {table} WHERE rating>{lower_limit} and rating<={higher_limit}'
    c.execute(QUERY_0.format(iter=0, table=ratingstablename, lower_limit=0, higher_limit=stepsize))
    for i in range(1, numberofpartitions):
        c.execute(QUERY_1.format(iter=i, table=ratingstablename, lower_limit=(i * stepsize), higher_limit=((i + 1) * stepsize)))
    c.execute("CREATE TABLE IF NOT EXISTS partition_meta(p_id INT, p_count INT)")
    c.execute('INSERT INTO partition_meta VALUES ({p_id}, {p_count})'.format(p_id=1, p_count=numberofpartitions))
    c.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    QUERY = '''
        CREATE TABLE rrobin_part{iter} AS 
        SELECT userid, movieid, rating 
        FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER() as rowid FROM {table}) AS temp
        WHERE mod(temp.rowid-1,{count_of_partitions}) = {iter_2}'''

    c = openconnection.cursor()
    for i in range(0, numberofpartitions):
        c.execute(QUERY.format(iter=i, table=ratingstablename, count_of_partitions=numberofpartitions, iter_2=i))
    c.execute("CREATE TABLE IF NOT EXISTS partition_meta(p_id INT, p_count INT)")
    c.execute('INSERT INTO partition_meta VALUES ({p_id}, {p_count})'.format(p_id=1, p_count=numberofpartitions))
    c.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    c = openconnection.cursor()
    c.execute('''INSERT INTO {table} VALUES ({u_id},{m_id},{rtg})'''.format(table=ratingstablename, u_id=userid, m_id=itemid, rtg=rating))
    c.execute('''SELECT * FROM {0} '''.format(ratingstablename))
    records_len = len(c.fetchall())
    c.execute('''SELECT p_count FROM partition_meta WHERE p_id = 1''')
    temp = c.fetchone()
    num_partitions = temp[0]
    frag_id = (records_len - 1) % num_partitions
    c.execute('''INSERT INTO rrobin_part{iter} VALUES ({u_id},{m_id},{rtg})'''.format(iter=frag_id, u_id=userid, m_id=itemid, rtg=rating))
    c.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    c = openconnection.cursor()
    highest_rating = 5.0
    c.execute('''SELECT p_count FROM partition_meta WHERE p_id = 1''')
    temp = c.fetchone()
    num_partitions = temp[0]
    c.execute('''INSERT INTO {0} VALUES ({1},{2},{3})'''.format(ratingstablename, userid, itemid, rating))
    QUERY = '''INSERT INTO range_part{iter} VALUES ({u_id},{m_id},{rtg})'''
    limit = highest_rating / num_partitions

    for i in range(0, num_partitions):
        if i == 0:
            if i * limit <= rating <= (i + 1) * limit:
                c.execute(QUERY.format(iter=i, u_id=userid, m_id=itemid, rtg=rating))
        else:
            if i * limit < rating <= (i + 1) * limit:
                c.execute(QUERY.format(iter=i, u_id=userid, m_id=itemid, rtg=rating))
    c.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
