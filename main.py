import pickle

import mcdm
import pandas as pd
import psycopg2
from flask import Flask, request, render_template, redirect, url_for

from timeAwareQOSPrediction import get_time_aware_Qos_prediction
from userSimilarity import get_similarity_matrix

app = Flask(__name__)

connection = psycopg2.connect(user="postgres",
                              password='postgres',
                              host="127.0.0.1",
                              port="5432",
                              database="webservicerecommendation")


@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == "POST":
        location = request.form.get("location").strip()
        category = request.form.get("category").strip()
        user_id = int(request.form.get("user_id").strip())
        print(user_id, category, location)
        cursor = connection.cursor()
        user_ids, service_ids = get_similarity_matrix(cursor, location, category, user_id)
        # predicted_qos = pickle.load(open('results.p', 'rb'))

        # Comment out for just checking results
        predicted_qos = get_time_aware_Qos_prediction(cursor, user_country=location, service_category=category, user_id=user_id)
        pickle.dump(predicted_qos, open("results2.p", "wb"))

        ranking = mcdm.rank(predicted_qos, alt_names=service_ids, is_benefit_x=[False, True], s_method="TOPSIS",
                            n_method="Vector")
        candidates = ", ".join([str(i[0]) for i in ranking][:5])
        query = "Select service_id, wsdl_address from webservices where service_id in (" + candidates + ")"
        cursor.execute(query)

        df = pd.DataFrame(cursor.fetchall(), columns=["Service Id", "WSDL Address"])
        return display_page(df)
    return render_template("home.html")


@app.route("/display_page", methods=["GET", "POST"])
def display_page(services_df):
    return render_template("display_page.html", data=services_df)


if __name__ == '__main__':
    app.run()


def print_mae(cursor, predicted_qos, user_id, service_ids):
    candidates = ", ".join([str(i) for i in service_ids])
    query = "Select response_time from rtmatrix where service_id in (" + candidates + ") and user_id = " + \
            str(user_id)
    cursor.execute(query)
    df_rt = pd.DataFrame(cursor.fetchall(), columns=["response_time"])

    query = "Select throughput from tpmatrix where service_id in (" + candidates + ") and user_id = " + \
            str(user_id)
    cursor.execute(query)
    df_tp = pd.DataFrame(cursor.fetchall(), columns=["throughput"])
    rt_mae, tp_mae = get_mae(df_rt, df_tp, predicted_qos)
    print("MAE for Response Time : " + str(rt_mae))
    print("MAE for Throughput : " + str(tp_mae))


def get_mae(df_rt, df_tp, predicted_qos):
    rt_mae = 0
    tp_mae = 0
    count = 0
    for index, row in df_rt.iterrows():
        rt_mae += abs(predicted_qos[index][0] - row['response_time'])
        count += 1
    rt_mae = rt_mae / count
    for index, row in df_tp.iterrows():
        tp_mae += abs(predicted_qos[index][1] - row['throughput'])
    print(tp_mae, count)
    tp_mae = tp_mae / count
    return rt_mae, tp_mae