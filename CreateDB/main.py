import psycopg2

from CreateDB.createDB import create_db


def main():
    username = input("Enter the user name : ")
    password = input("Enter the password : ")
    host = input("Enter host: ")
    port = input("Enter port: ")
    database = input("Enter database name: ")
    connection = psycopg2.connect(user=username,
                                  password=password,
                                  host=host,
                                  port=port,
                                  database=database)

    cursor = connection.cursor()
    create_db(cursor)
    connection.commit()


if __name__ == '__main__':
    main()
