import psycopg2
import mcdm
import pandas as pd

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
                                  password='postgres',
                                  host="127.0.0.1",
                                  port="5432",
                                  database="webservicerecommendation")

    location = input("Enter user location : ").strip()
    category = input("Enter service category : ").strip()
    cursor = connection.cursor()
    user_ids, service_ids = get_similarity_matrix(cursor, location, category)
    predicted_qos = get_time_aware_Qos_prediction(cursor, user_country=location, service_category=category)
    ranking = mcdm.rank(predicted_qos, alt_names=service_ids, is_benefit_x=[False, True], s_method="TOPSIS",
                    n_method="Vector")
    candidates = ", ".join([str(i[0]) for i in ranking])
    print(candidates)
    query = "Select service_id, wsdl_address from webservices where service_id in (" + candidates + ")"
    cursor.execute(query)

    df = pd.DataFrame(cursor.fetchall(), columns=["Service Id", "WSDL Address"])
    print(df.head(5))



if __name__ == "__main__":
    main()
