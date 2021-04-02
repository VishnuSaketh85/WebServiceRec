import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pickle
import math
from tqdm import tqdm
import psycopg2


def get_similarity_matrix(cursor):
    query = """select t3.user_id, t3.service_id, t3.timeslice_id, t3.response_time from 
                (select user_id from users where country = 'United States')t4 
                join (select t1.user_id, t1.service_id, t1.timeslice_id, t1.response_time
                from rt_sliced t1 join (SELECT service_id FROM webservices 
                where 'Sports' = ANY(category))t2 on t1.service_id
                = t2.service_id)t3 on t4.user_id = t3.user_id
                ORDER BY t3.user_id, t3.service_id, t3.timeslice_id;"""
    cursor.execute(query)
    data = pd.DataFrame(cursor.fetchall(), columns=["User ID", "Service ID", "Timeslice Id", "response_time"])
    userServiceDictRt = data.groupby(['User ID', 'Service ID']).apply(
        func=lambda x: x[['Timeslice Id', 'response_time']].to_numpy()).to_dict()

    userct = data['User ID'].nunique()
    servicect = data['Service ID'].nunique()
    print(userct, servicect)
    tb_current = max(data['Timeslice Id'].max(), data['Timeslice Id'].max())
    userRtAvg = data[['User ID', 'response_time']].groupby(['User ID']).agg('mean').to_numpy()[:, 0]
    serviceRtAvg = data[['Service ID', 'response_time']].groupby(['Service ID']).agg('mean').to_numpy()[:, 0]
    computeSimilarityMatrix(dic=userServiceDictRt, user_flag=True, qos_avg=userRtAvg, t_current=tb_current,
                            alpha=1, beta=1, QOS_type="response_time", servicect=servicect, userct=userct)
    print("Similarity Computation done")


def computeSimilarityMatrix(dic, user_flag, qos_avg, t_current, alpha, beta, QOS_type, userct, servicect):
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
