import os
import sys
import threading
import time

import mysql.connector
from filecleaver import cleave # https://github.com/fprimex/filecleaver
from mysql.connector import errorcode, pooling


# Returns current time, platform independent
def get_time():
    if sys.platform == 'win32':
        # On Windows, the best timer is time.clock
        return time.clock()
    else:
        # On most other platforms the best timer is time.time
        return time.time()


# Loads the data to the table
def load_data(csv_file, table):
    global total_rows
    print("Starting new load data thread")
    try:
        conn = conn_pool.get_connection()
        cursor = conn.cursor()
    except mysql.connector.Error as e:
        if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Username or password incorrect")
        elif e.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(e)

    field_termination = "','"
    field_enclosing = "'\"'"
    line_termination = "'\\n'"
    sql = "LOAD DATA INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY {} OPTIONALLY ENCLOSED BY {} " \
          "LINES TERMINATED BY {}".format(csv_file, table, field_termination, field_enclosing, line_termination)
    print(sql)
    print("Loading {} into table {}".format(csv_file, table))
    t_start_time = get_time()
    cursor.execute(sql)
    print("SQL query: {}".format(cursor.statement))
    row_count = cursor.rowcount
    conn.commit()
    t_end_time = get_time()
    thread_duration = round(t_end_time - t_start_time, 2)
    total_rows += row_count
    print("Finished loading {} to {}, inserted a total of {} rows in {} seconds".format(csv_file,
                                                                                        table,
                                                                                        row_count,
                                                                                        thread_duration))
    cursor.close()
    conn.close()


# Write file chunks
def write_chunk(file_name, dst_dir, reader, i):
    outfile = os.path.join(dst_dir, '{}_{}.csv'.format(file_name, i))
    if os.path.exists(outfile):
        os.remove(outfile)
    with reader.open() as src, open(outfile, 'wb') as dst:
        dst.write(src.read())
        tmp_filelist.append(outfile)
        return 'Chunk #{} was {} bytes'.format(i, src.end - src.start)


# Globals
dbconfig = {
    'host': '127.0.0.1',
    'user': 'test_db_user',
    'password': 'steal_this_pwd',
    'database': 'test_db'
}

csv_file = os.path.abspath(sys.argv[1])
table = sys.argv[2]
thread_qty = int(sys.argv[3])
fname, ext = os.path.splitext(os.path.basename(csv_file))
tmp_dir = '/tmp'
tmp_filelist = []

csv_file_size = os.path.getsize(csv_file)

start_time = get_time()
total_duration = 0
total_rows = 0

if thread_qty > 1:
    # Splits the file into 'thread_qty' threads
    s_start_time = get_time()
    print('Splitting {} into {} chunks'.format(csv_file, thread_qty))
    thread_list = []
    readers = cleave(csv_file, thread_qty)
    for i, reader in enumerate(readers):
        t = threading.Thread(target=write_chunk, args=(fname, tmp_dir, reader, i))
        thread_list.append(t)
    # Start file-splitting the threads
    for thread in thread_list:
        thread.start()
    # Join the file-splitting threads to the main one
    for thread in thread_list:
        thread.join()
    s_end_time = get_time()
    s_total_duration = round(s_end_time - s_start_time, 2)
    print('Done splitting the file, took {} seconds'.format(s_total_duration))

    # Create a new thread for each generated csv file
    thread_list = []
    for file in tmp_filelist:
        print('Creating a new thread to load {}'.format(file))
        t = threading.Thread(target=load_data, args=(file, table))
        thread_list.append(t)

    # Creates a pool of connections to MySQL so that each thread can have it's own
    conn_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="dbpool",
                                                            pool_size=thread_qty,
                                                            **dbconfig)

    # Start the file-loading threads
    for thread in thread_list:
        thread.start()
    # Join the file-loading threads to the main one
    for thread in thread_list:
        thread.join()
    end_time = get_time()
    total_duration = round(end_time - start_time, 2)
    print("Finished parallel loading of {} rows in {} seconds".format(total_rows, total_duration))
else:
    # Creates a pool with a single connection to MySQL
    conn_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="dbpool",
                                                            pool_size=1,
                                                            **dbconfig)
    print("Single-threaded loading, not splitting the file")
    load_data(csv_file, table)
    end_time = get_time()
    total_duration = round(end_time - start_time, 2)
    print("Finished single-threaded loading of {} rows in {} seconds".format(total_rows, total_duration))
