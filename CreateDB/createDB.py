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
                  "ALTER TABLE webservices add category VARCHAR(100)" \
                  "ALTER TABLE webservices" \
                  "ALTER COLUMN long TYPE float USING NULLIF(long, 'null')::float;" \
                  "ALTER TABLE webservices" \
                  "ALTER COLUMN lat TYPE float USING NULLIF(lat, 'null')::float;"

    cursor.execute(alter_query)
