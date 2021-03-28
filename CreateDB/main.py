import psycopg2

from CreateDB.createDB import insert_data


def main():
    # useranme = input("Enter the user name : ")
    # password = input("Enter the password : ")
    # host = input("Enter host: ")
    # port = input("Enter port: ")
    # database = input("Enter database name: ")
    # connection = psycopg2.connect(user=username,
    #                               password=password,
    #                               host=host,
    #                               port=port,
    #                               database=database)
    connection = psycopg2.connect(user="postgres",
                                  password='postgres',
                                  host="127.0.0.1",
                                  port="5432",
                                  database="webservicerecommendation")
    cursor = connection.cursor()
    insert_data(cursor)
    connection.commit()


if __name__ == '__main__':
    main()
