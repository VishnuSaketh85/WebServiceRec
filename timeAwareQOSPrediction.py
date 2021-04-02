import pickle
import numpy as np
import pandas as pd


def open_pickle(file):
    user_response = pickle.load(open(file, 'rb'))
    return user_response


def calc_time_factor_user(timeslice, tcurrent=63):
    gamma = 0.085
    return np.exp(-gamma * np.abs(tcurrent - timeslice))


def calc_time_factor_service(timeslice, tcurrent=63):
    gamma = 0.085
    return np.exp(-gamma * np.abs(tcurrent - timeslice))


def time_aware_qos_user(cursor, user_sim_matrix_res, service_category, user_country, qos_type="Response Time"):
    SQL_QUERY = ""
    if qos_type == 'Response Time':
        SQL_QUERY = "SELECT R.user_id, W.service_id, R.timeslice_id, R.response_time from webservices W " \
                    "JOIN rt_sliced R using (service_id) " \
                    "where '" + service_category + "' = ANY(W.category) " \
                    "AND R.user_id IN " \
                    "(SELECT user_id from USERS where country = '" + user_country +"')" \
                    "ORDER BY R.user_id, W.service_id, R.timeslice_id;"

    elif qos_type == 'Throughput':
        SQL_QUERY = "SELECT T.user_id, W.service_id, T.timeslice_id, T.throughput from webservices W " \
                    "JOIN tp_sliced T using (service_id) " \
                    "where '" + service_category + "' = ANY(W.category) " \
                    "AND T.user_id IN " \
                    "(SELECT user_id from USERS where country = '" + user_country + "')" \
                    "ORDER BY T.user_id, W.service_id, T.timeslice_id;"

    cursor.execute(SQL_QUERY)

    response_time_df = pd.DataFrame(cursor.fetchall(), columns=["User Id", "Service Id", "Timeslice Id", qos_type])

    rt_agg = response_time_df[['User Id', qos_type]].groupby('User Id').agg('mean')
    print(rt_agg)

    users = set(response_time_df['User Id'])
    print(len(users), users)

    services = set(response_time_df['Service Id'])
    print(len(services), services)

    qos_matrix = np.zeros((len(users), len(services)))

    for idx, i in enumerate(list(users)):
        i_serv = response_time_df[response_time_df['User Id'] == i]
        qos_i = rt_agg[rt_agg.index == i].iloc[0][qos_type]
        for idx_s, service in enumerate(list(services)):
            numerator, denominator = 0, 0
            for j, qos_sim_j in enumerate(user_sim_matrix_res[i]):

                # if users are same or QOS value is less than zero or there are no services by user j
                j_user = response_time_df[(response_time_df['User Id'] == j) &
                                          (response_time_df['Service Id'] == service)]
                if i == j or qos_sim_j <= 0 or len(j_user) <= 0:
                    continue

                # average qos for user j
                qos_j = rt_agg[rt_agg.index == j]
                qos_j = qos_j.iloc[0][qos_type]

                # Calculate time factor f3
                time_factor_f3 = j_user[qos_type].apply(lambda x: calc_time_factor_user(x))

                qos_j_k = np.abs(j_user[qos_type].subtract(qos_j))

                numerator += (qos_sim_j * (np.sum((time_factor_f3 * qos_j_k)) / j_user.shape[0]))
                denominator += (qos_sim_j * (np.sum(time_factor_f3) / j_user.shape[0]))

            frac = (numerator / denominator)
            qos_matrix[idx][idx_s] = qos_i + frac

    print(qos_matrix)
    return qos_matrix


def time_aware_qos_service(cursor, service_sim_matrix, service_category, user_country, qos_type="Response Time"):
    SQL_QUERY = ""
    if qos_type == 'Response Time':
        SQL_QUERY = "SELECT R.user_id, W.service_id, R.timeslice_id, R.response_time from webservices W " \
                    "JOIN rt_sliced R using (service_id) " \
                    "where '" + service_category + "' = ANY(W.category) " \
                    "AND R.user_id IN " \
                    "(SELECT user_id from USERS where country = '" + user_country +"')" \
                    "ORDER BY R.user_id, W.service_id, R.timeslice_id;"

    elif qos_type == 'Throughput':
        SQL_QUERY = "SELECT T.user_id, W.service_id, T.timeslice_id, T.throughput from webservices W " \
                    "JOIN tp_sliced T using (service_id) " \
                    "where '" + service_category + "' = ANY(W.category) " \
                    "AND T.user_id IN " \
                    "(SELECT user_id from USERS where country = '" + user_country + "')" \
                    "ORDER BY T.user_id, W.service_id, T.timeslice_id;"

    cursor.execute(SQL_QUERY)

    response_time_df = pd.DataFrame(cursor.fetchall(), columns=["User Id", "Service Id", "Timeslice Id", qos_type])

    rt_agg = response_time_df[['Service Id', qos_type]].groupby('Service Id').agg('mean')
    print(rt_agg)

    users = set(response_time_df['User Id'])
    print(len(users), users)

    services = set(response_time_df['Service Id'])
    print(len(services), services)

    qos_matrix = np.zeros((len(users), len(services)))

    for idx_s, service_s in enumerate(list(services)):
        user_s = response_time_df[response_time_df['Service Id'] == service_s]
        qos_i = rt_agg[rt_agg.index == service_s].iloc[0][qos_type]
        for idx_i, user_i in enumerate(list(users)):
            numerator, denominator = 0, 0
            for idx_r, qos_sim_r in enumerate(service_sim_matrix[service_s]):

                # if users are same or QOS value is less than zero or there are no services by user idx_r
                service_r = response_time_df[(response_time_df['User Id'] == idx_r) &
                                          (response_time_df['Service Id'] == user_i)]
                if service_s == idx_r or qos_sim_r <= 0 or len(service_r) <= 0:
                    continue

                # average qos for user idx_r
                qos_j = rt_agg[rt_agg.index == idx_r]
                qos_j = qos_j.iloc[0][qos_type]

                # Calculate time factor f3
                time_factor_f3 = service_r[qos_type].apply(lambda x: calc_time_factor_service(x))

                qos_j_k = np.abs(service_r[qos_type].subtract(qos_j))

                numerator += (qos_sim_r * (np.sum((time_factor_f3 * qos_j_k)) / service_r.shape[0]))
                denominator += (qos_sim_r * (np.sum(time_factor_f3) / service_r.shape[0]))

            frac = (numerator / denominator)
            qos_matrix[idx_s][idx_i] = qos_i + frac

    print(qos_matrix)
    return qos_matrix


def calculate_confidence_weights(sim_matrix):
    if len(sim_matrix) == 0:
        return 0
    conf = np.apply_along_axis(lambda x: np.sum(np.square(x) / np.sum(x)), axis=1, arr=sim_matrix)
    return np.sum(conf)


def get_prediction_weights_user(confidence_weight_user, confidence_weight_service, l=0.085):
    x = l * confidence_weight_user
    return x / (x + (1 - l) * confidence_weight_service)


def get_prediction_weights_service(confidence_weight_user, confidence_weight_service, l=0.085):
    x = (1 - l) * confidence_weight_service
    return x / (x + (l * confidence_weight_user))


def get_prediction_weights(user_sim_matrix_rt, user_sim_matrix_tp, service_sim_matrix_rt, service_sim_matrix_tp):
    # Calculate confidence weights
    conf_u_rt = calculate_confidence_weights(user_sim_matrix_rt)
    conf_u_tp = calculate_confidence_weights(user_sim_matrix_tp)
    conf_s_rt = calculate_confidence_weights(service_sim_matrix_rt)
    conf_s_tp = calculate_confidence_weights(service_sim_matrix_tp)

    # Calculate prediction weights
    pred_wt_u_rt = get_prediction_weights_user(conf_u_rt, conf_s_rt)
    pred_wt_u_tp = get_prediction_weights_user(conf_u_tp, conf_s_tp)
    pred_wt_s_rt = get_prediction_weights_service(conf_u_rt, conf_s_rt)
    pred_wt_s_tp = get_prediction_weights_service(conf_u_tp, conf_s_tp)
    return pred_wt_u_rt, pred_wt_u_tp, pred_wt_s_rt, pred_wt_s_tp


def get_final_qos_values():
    pass


def get_time_aware_Qos_prediction(cursor, service_category="Entertainment", user_country='United States'):

    # THESE SHOULD BE FROM THE O/P of SIMILARITY MATRIX.. hardcoding it for now..
    user_sim_matrix_res = open_pickle("./user_similarity_matrix_Response_Time.p")
    user_sim_matrix_tp = []
    service_sim_matrix_rt = []
    service_sim_matrix_tp = []

    # Time aware qos user
    qos_matrix_u_rt = time_aware_qos_user(cursor, user_sim_matrix_res, service_category, user_country, "Response Time")
    qos_matrix_u_tp = time_aware_qos_user(cursor, user_sim_matrix_tp, service_category, user_country, "Throughput")

    # Time aware qos service
    qos_matrix_s_tp = time_aware_qos_service(cursor, service_sim_matrix_rt, service_category, user_country, "Response Time")
    qos_matrix_s_tp = time_aware_qos_service(cursor, service_sim_matrix_tp, service_category, user_country, "Throughput")

    # Calculate prediction weights for final QOS value predictions
    pred_wt_u_rt, pred_wt_u_tp, pred_wt_s_rt, pred_wt_s_tp = get_prediction_weights(user_sim_matrix_res,
                                                                                    user_sim_matrix_tp,
                                                                                    service_sim_matrix_rt,
                                                                                    service_sim_matrix_tp)






