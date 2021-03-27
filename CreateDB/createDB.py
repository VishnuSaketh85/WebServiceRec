import os

cwd = os.getcwd()


def create_db(cursor):
    create_user_table(cursor)
    create_ws_table(cursor)

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
                   "long TEXT)"
    cursor.execute(create_query)

    copy_query = "COPY webservices FROM '" + cwd + "/wslist.txt" + "' WITH (FORMAT csv, HEADER true, DELIMITER E'\t')"


    cursor.execute(copy_query)
