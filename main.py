import psycopg2

from userSimilarity import get_similarity_matrix
from timeAwareQOSPrediction import get_time_aware_Qos_prediction


def main():
    # username = input("Enter the user name : ")
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
                                  password='Slayer@45',
                                  host="127.0.0.1",
                                  port="5432",
                                  database="vishnusaketh")

    location = input("Enter user location : ").strip()
    category = input("Enter service category : ").strip()
    cursor = connection.cursor()
    get_similarity_matrix(cursor, location, category)
    get_time_aware_Qos_prediction(cursor, user_country=location, service_category=category)


if __name__ == "__main__":
    main()
