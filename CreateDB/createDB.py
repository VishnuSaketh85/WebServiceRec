import os
import time

cwd = os.getcwd()


def create_db(cursor):
    create_user_table(cursor)

    create_ws_table(cursor)

    print("Creating rtMatrix table.....")
    start_time = time.time()
    create_rt_table(cursor)
    print("Created rtMatrix table in " + str(time.time() - start_time) + "s")

    print("Creating rtMatrix table.....")
    start_time = time.time()
    create_tp_table(cursor)
    print("Created tpMatrix table in " + str(time.time() - start_time) + "s")

    print("Creating rt_sliced table.....")
    start_time = time.time()
    create_rt_sliced_table(cursor)
    print("Created rt_sliced table in " + str(time.time() - start_time) + "s")

    print("Creating tp_sliced table.....")
    start_time = time.time()
    create_tp_sliced_table(cursor)
    print("Created tp_sliced table in " + str(time.time() - start_time) + "s")


def create_user_table(cursor):
    create_query = "CREATE TABLE IF NOT EXISTS USERS(" \
                   "user_id INT PRIMARY KEY," \
                   "ip_address VARCHAR(50)," \
                   "country VARCHAR(50)," \
                   "ip_no VARCHAR(50)," \
                   "autonomous_systems TEXT," \
                   "lat FLOAT," \
                   "long FLOAT)"
    cursor.execute(create_query)

    copy_query = "COPY users(user_id, ip_address, country, ip_no, autonomous_systems" \
                 ",lat, long) FROM '" + cwd + "/userlist.txt" + "' WITH (FORMAT csv, HEADER true, DELIMITER E'\t')"
    cursor.execute(copy_query)


def create_ws_table(cursor):
    create_query = "CREATE TABLE IF NOT EXISTS WebServices_temp(" \
                   "service_id INT," \
                   "wsdl_address TEXT," \
                   "service_provider TEXT," \
                   "ip_address VARCHAR(100)," \
                   "country VARCHAR(50)," \
                   "ip_no VARCHAR(50)," \
                   "autonomous_systems TEXT," \
                   "lat TEXT," \
                   "long TEXT);" \
                   "CREATE TABLE IF NOT EXISTS WebServices(" \
                   "service_id INT PRIMARY KEY," \
                   "wsdl_address TEXT," \
                   "service_provider TEXT," \
                   "ip_address VARCHAR(100)," \
                   "country VARCHAR(50)," \
                   "ip_no VARCHAR(50)," \
                   "autonomous_systems TEXT," \
                   "lat TEXT," \
                   "long TEXT);"
    cursor.execute(create_query)

    copy_query = "COPY webservices_temp FROM '" + cwd + "/wslist.txt" + \
                 "' WITH (FORMAT csv, HEADER true, DELIMITER E'\t')"
    cursor.execute(copy_query)

    alter_query = "INSERT INTO webservices (SELECT DISTINCT ON(service_id) service_id, wsdl_address," \
                  "service_provider, ip_address, country, ip_no, autonomous_systems, lat, " \
                  "long from webservices_temp);" \
                  "DROP TABLE webservices_temp;" \
                  "ALTER TABLE webservices add category VARCHAR(100);" \
                  "ALTER TABLE webservices " \
                  "ALTER COLUMN long TYPE float USING NULLIF(long, 'null')::float;" \
                  "ALTER TABLE webservices " \
                  "ALTER COLUMN lat TYPE float USING NULLIF(lat, 'null')::float;"

    cursor.execute(alter_query)


def create_rt_table(cursor):
    create_query = "CREATE TABLE IF NOT EXISTS rtMatrix(" \
                   "user_id INT REFERENCES users(user_id)," \
                   "service_id INT REFERENCES webservices(service_id)," \
                   "response_time FLOAT," \
                   "PRIMARY KEY(user_id, service_id))"
    cursor.execute(create_query)

    f = open(cwd + '/rtMatrix.txt', 'r')
    lines = f.readlines()
    values = []
    for user_id in range(0, len(lines)):
        line = lines[user_id].strip().split('\t')
        for service_id in range(0, len(line)):
            if service_id == 4700 or service_id == 4701:
                continue
            values.append([user_id, service_id, line[service_id]])
    args_str = ','.join(cursor.mogrify("(%s,%s,%s)", x).decode("utf-8") for x in values)
    cursor.execute("INSERT INTO rtMatrix VALUES " + args_str)


def create_tp_table(cursor):
    create_query = "CREATE TABLE IF NOT EXISTS tpMatrix(" \
                   "user_id INT REFERENCES users(user_id)," \
                   "service_id INT REFERENCES webservices(service_id)," \
                   "throughput FLOAT," \
                   "PRIMARY KEY(user_id, service_id))"
    cursor.execute(create_query)

    f = open(cwd + '/tpMatrix.txt', 'r')
    lines = f.readlines()
    values = []
    for user_id in range(0, len(lines)):
        line = lines[user_id].strip().split('\t')
        for service_id in range(0, len(line)):
            if service_id == 4700 or service_id == 4701:
                continue
            values.append([user_id, service_id, line[service_id]])
    args_str = ','.join(cursor.mogrify("(%s,%s,%s)", x).decode("utf-8") for x in values)
    cursor.execute("INSERT INTO tpMatrix VALUES " + args_str)


def create_rt_sliced_table(cursor):
    create_query = "CREATE TABLE IF NOT EXISTS rt_sliced(" \
                   "user_id INT REFERENCES users(user_id)," \
                   "service_id INT REFERENCES webservices(service_id)," \
                   "timeslice_id INT," \
                   "response_time FLOAT," \
                   "PRIMARY KEY(user_id, service_id, timeslice_id));"
    cursor.execute(create_query)

    copy_query = "COPY rt_sliced " \
                 "FROM '" + cwd + "/rt_sliced.csv'" \
                 "DELIMITER ',';"
    cursor.execute(copy_query)


def create_tp_sliced_table(cursor):
    create_query = "CREATE TABLE IF NOT EXISTS tp_sliced(" \
                   "user_id INT REFERENCES users(user_id)," \
                   "service_id INT REFERENCES webservices(service_id)," \
                   "timeslice_id INT," \
                   "throughput FLOAT," \
                   "PRIMARY KEY(user_id, service_id, timeslice_id));"
    cursor.execute(create_query)

    copy_query = "COPY tp_sliced " \
                 "FROM '" + cwd + "/tp_sliced.csv'" \
                                  "DELIMITER ',';"
    cursor.execute(copy_query)
