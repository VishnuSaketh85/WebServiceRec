import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pickle
import math
from tqdm import tqdm
import psycopg2

delta = 0.00000000000001


def get_similarity_matrix(cursor, location, category, user_id):
    user_response_time_query = """select t3.user_id, t3.service_id, t3.timeslice_id, t3.response_time from 
                (select user_id from users where country = '""" + location + """')t4 
                join (select t1.user_id, t1.service_id, t1.timeslice_id, t1.response_time
                from rt_sliced t1 join (SELECT service_id FROM webservices 
                where '""" + category + """' = ANY(category))t2 on t1.service_id
                = t2.service_id)t3 on t4.user_id = t3.user_id
                ORDER BY t3.user_id, t3.service_id, t3.timeslice_id;"""
    user_throughput_query = """select t3.user_id, t3.service_id, t3.timeslice_id, t3.throughput from 
                (select user_id from users where country = '""" + location + """')t4 
                join (select t1.user_id, t1.service_id, t1.timeslice_id, t1.throughput
                from tp_sliced t1 join (SELECT service_id FROM webservices 
                where '""" + category + """' = ANY(category))t2 on t1.service_id
                = t2.service_id)t3 on t4.user_id = t3.user_id
                ORDER BY t3.user_id, t3.service_id, t3.timeslice_id;"""
    cursor.execute(user_response_time_query)
    rt_data = pd.DataFrame(cursor.fetchall(), columns=["User ID", "Service ID", "Timeslice Id", "response_time"])
    userServiceDictRt = rt_data.groupby(['User ID', 'Service ID']).apply(
        func=lambda x: x[['Timeslice Id', 'response_time']].to_numpy()).to_dict()

    cursor.execute(user_throughput_query)
    tp_data = pd.DataFrame(cursor.fetchall(), columns=["User ID", "Service ID", "Timeslice Id", "throughput"])
    userServiceDictTp = rt_data.groupby(['User ID', 'Service ID']).apply(
        func=lambda x: x[['Timeslice Id', 'response_time']].to_numpy()).to_dict()

    userct = rt_data['User ID'].nunique()
    servicect = rt_data['Service ID'].nunique()
    user_ids = sorted(list(set(rt_data['User ID'].tolist())))
    service_ids = sorted(list(set(rt_data['Service ID'].tolist())))
    tb_current = max(rt_data['Timeslice Id'].max(), rt_data['Timeslice Id'].max())
    userRtAvg = rt_data[['User ID', 'response_time']].groupby(['User ID']).agg('mean').to_numpy()[:, 0]
    serviceRtAvg = rt_data[['Service ID', 'response_time']].groupby(['Service ID']).agg('mean').to_numpy()[:, 0]
    computeSimilarityMatrix(dic=userServiceDictRt, user_flag=True, qos_avg=userRtAvg, t_current=tb_current,
                            alpha=1, beta=1, QOS_type="response_time", servicect=servicect, userct=userct, ids=user_ids,
                            user_id=user_id)
    computeSimilarityMatrix(dic=userServiceDictRt, user_flag=False, qos_avg=serviceRtAvg, t_current=tb_current,
                            alpha=1, beta=1, QOS_type="response_time", servicect=servicect, userct=userct, ids=user_ids,
                            user_id=user_id)

    userct = tp_data['User ID'].nunique()
    servicect = tp_data['Service ID'].nunique()
    tb_current = max(rt_data['Timeslice Id'].max(), rt_data['Timeslice Id'].max())
    userTpAvg = tp_data[['User ID', 'throughput']].groupby(['User ID']).agg('mean').to_numpy()[:, 0]
    serviceTpAvg = tp_data[['Service ID', 'throughput']].groupby(['Service ID']).agg('mean').to_numpy()[:, 0]
    computeSimilarityMatrix(dic=userServiceDictTp, user_flag=True, qos_avg=userTpAvg, t_current=tb_current,
                            alpha=1, beta=1, QOS_type="throughput", servicect=servicect, userct=userct, ids=user_ids,
                            user_id=user_id)
    computeSimilarityMatrix(dic=userServiceDictTp, user_flag=False, qos_avg=serviceTpAvg, t_current=tb_current,
                            alpha=1, beta=1, QOS_type="throughput", servicect=servicect, userct=userct, ids=user_ids,
                            user_id=user_id)

    print("Similarity Computation done")
    return user_ids, service_ids


def computeSimilarityMatrix(dic, user_flag, qos_avg, t_current, alpha, beta, QOS_type, userct, servicect, ids, user_id):
    if user_flag:
        ct = userct
        str_name = 'user'
    else:
        ct = servicect
        str_name = 'service'
    qos_matrix = np.zeros((ct, ct))

    outer = np.array(list(range(ct)))
    inner = np.array(list(range(ct)))
    cartesian = np.transpose([np.tile(outer, len(inner)), np.repeat(inner, len(outer))])

    for idx in tqdm(range(len(cartesian)), desc='User Spaces'):
        id1, id2 = cartesian[idx][0], cartesian[idx][1]
        if id1 < id2:
            qos_matrix[id1][id2] = getSimilarity(dic, user_flag, id1, id2, qos_avg[id1], qos_avg[id2], t_current, alpha,
                                                 beta, servicect, userct)
            qos_matrix[id2][id1] = qos_matrix[id1][id2]

    qos_matrix = random_walk(qos_matrix, user_id, user_flag)
    if user_flag:
        s_user_id = []
        for i in range(len(qos_matrix)):
            s_user_id.append((ids[i], qos_matrix[i]))
        qos_matrix = s_user_id
    pickle.dump(qos_matrix, open(str_name + "_similarity_matrix_" + QOS_type + ".p", "wb"))

    return qos_matrix


def getSimilarity(dic, user_flag, id1, id2, q_avg1, q_avg2, t_current, alpha, beta, servicect, userct):
    if user_flag:
        common_item1, common_item2, unique_ct1, unique_ct2 = getCoInvokedServices(dic, id1, id2, servicect=servicect)
    else:
        common_item1, common_item2, unique_ct1, unique_ct2 = getCommonUsers(dic, id1, id2, userct=userct)

    commonality = len(common_item1)

    if commonality == 0:
        return 0

    num1_sum, den1_sum, den2_sum = 0, 0, 0

    for idx in range(commonality):
        (num1, den1, den2) = getSingleItemCalculations(dic[common_item1[idx]], dic[common_item2[idx]], q_avg1, q_avg2,
                                                       t_current, alpha, beta)
        num1_sum += num1
        den1_sum += den1
        den2_sum += den2

    w = num1_sum / ((den1_sum * den2_sum) ** 0.5)
    w = 2 * (commonality) * w / ((commonality + unique_ct1) + (commonality + unique_ct2))

    del common_item1, common_item2, commonality
    return w


def getCommonUsers(dic, serv1, serv2, userct):
    users1 = []
    users2 = []

    users1_unique_ct = 0
    users2_unique_ct = 0

    for user_idx in range(userct):
        if (user_idx, serv1) in dic and (user_idx, serv2) in dic:
            users1.append((user_idx, serv1))
            users2.append((user_idx, serv2))

        elif (user_idx, serv1) in dic:
            users1_unique_ct += 1
        elif (user_idx, serv2) in dic:
            users2_unique_ct += 1

    return users1, users2, users1_unique_ct, users2_unique_ct


def getCoInvokedServices(dic, uid1, uid2, servicect):
    services1 = []
    services2 = []

    services1_unique_ct = 0
    services2_unique_ct = 0

    for service_idx in range(servicect):
        if (uid1, service_idx) in dic and (uid2, service_idx) in dic:
            services1.append((uid1, service_idx))
            services2.append((uid2, service_idx))

        elif (uid1, service_idx) in dic:
            services1_unique_ct += 1
        elif (uid2, service_idx) in dic:
            services2_unique_ct += 1

    return services1, services2, services1_unique_ct, services2_unique_ct


def getSingleItemCalculations(qos1, qos2, q_avg1, q_avg2, t_current, alpha, beta):
    diff1 = qos1[:, 1] - q_avg1
    diff2 = qos2[:, 1] - q_avg2

    time1 = qos1[:, 0]
    time2 = qos2[:, 0]

    den1 = (diff1 * diff1).mean()
    den2 = (diff2 * diff2).mean()

    diff1_rep = diff1.repeat(len(diff2)).reshape(len(diff1), len(diff2))
    diff2_rep = diff2.repeat(len(diff1)).reshape(len(diff2), len(diff1))

    diff_prod = diff1_rep.transpose() * diff2_rep

    time1_rep = time1.repeat(len(time2)).reshape(len(time1), len(time2))
    time2_rep = time2.repeat(len(time1)).reshape(len(time2), len(time1))

    decay1 = np.exp(-alpha * np.abs(time1_rep.transpose() - time2_rep))
    decay2 = np.exp(-beta * np.abs(t_current - (time1_rep.transpose() + time2_rep) / 2))

    num1 = (diff_prod * decay1 * decay2).mean()

    del diff1, diff2, time1, time2, diff1_rep, diff2_rep, diff_prod, time1_rep, time2_rep, decay1, decay2

    return num1, den1, den2


def random_walk(qos_matrix, user_id, user_flag):
    original_qos_matrix = qos_matrix
    row_ct = qos_matrix.shape[0]
    col_ct = qos_matrix.shape[1]
    for i in range(row_ct):
        for j in range(col_ct):
            if qos_matrix[i, j] <= 0 or i == j:
                qos_matrix[i, j] = delta
    col_sums = [0] * col_ct
    for j in range(col_ct):
        for i in range(row_ct):
            col_sums[j] += qos_matrix[i, j]

    # normalizing
    for i in range(row_ct):
        for j in range(col_ct):
            qos_matrix[i, j] = qos_matrix[i, j] / col_sums[j]

    d = 0.85
    I = np.identity(row_ct)

    inverse_matrix = (1 - d) * np.linalg.inv(I - (d * qos_matrix))

    if user_flag:
        p = np.array([0] * col_ct)
        p[user_id] = 1

        r = np.matmul(inverse_matrix, np.transpose(p))
        k = 0
        summation = 0
        top_k = []
        for j in range(col_ct):
            if qos_matrix[user_id, j] > 0:
                k += 1
                top_k.append(j)
                summation += (qos_matrix[user_id, j] / r[j])
        s_user_id = (summation / k) * np.transpose(r)
        return s_user_id

    else:
        r = np.transpose(inverse_matrix)
        for i in range(row_ct):
            k = 0
            summation = 0
            top_k = []
            for j in range(col_ct):
                if original_qos_matrix[i, j] > 0:
                    k += 1
                    top_k.append(j)
                    summation += (original_qos_matrix[i, j] / r[i, j])
            for j in range(col_ct):
                r[i, j] = summation / k * r[i, j]
        return r






