# -- coding: utf-8 --
import numpy as np
import torch
import torch.nn as nn
import random
import copy
import networkx as nx
import Models as ms
import utilize as ult
import time

CosSim = nn.CosineSimilarity(dim=0, eps=1e-08)


def DIC(group_ids, item_id, model, A_norm, Ws):
    R_gg = model.social_neighbors_sparse_matrix
    groups_emb = model.final_group_embedding

    print(groups_emb.shape)
    item_emb = model.final_item_embedding[item_id]
    ProbGraph = generate_ProbGraph(R_gg, groups_emb, item_emb, A_norm, Ws)
    inf_num = simulate_dynamic(ProbGraph, group_ids.tolist())
    return inf_num


def preprocess(G):
    p = 0
    directedGraph = nx.DiGraph()
    for u in G.nodes():
        for v in G.neighbors(u):
            if (v != u):
                # propProb = G.number_of_edges(u, v) / G.in_degree(v)
                propProb = G.number_of_edges(u, v) / G.degree(v)
                directedGraph.add_edge(u, v, pp=propProb)
                # p += propProb
                # print(propProb)
    # print('平均阈值：', p/2939)
    return directedGraph


def compute_prob_edge(gi_emb, gj_emb, item_emb, A_i, weigth):
    A_i = 1
    W_i = A_i * CosSim(gi_emb, item_emb)
    Sim = CosSim(gi_emb, gj_emb)
    W_j = CosSim(gj_emb, item_emb)
    w1 = 0.1
    w2 = 0.7
    # w1 = weigth[0]
    # w2 = weigth[1]
    w3 = 1 - w1 - w2

    Prob = (w1 * A_i * W_i + w2 * Sim + w3 * W_j)
    # print(Prob)
    return Prob


def generate_ProbGraph(R_gg, group_embs, item_emb, As, Ws):
    value_list = []
    index_list = []
    group_num, _ = R_gg.shape

    print('group_emb', group_embs.shape)
    print('Computing')
    for g_i in range(group_num):
        for g_j in range(g_i, group_num):
            if g_j > 200 + g_i:
                break
            if R_gg[g_i][g_j] == 1:
                prob_ij = compute_prob_edge(group_embs[g_i], group_embs[g_j], item_emb, As[g_i], Ws)
                prob_ji = compute_prob_edge(group_embs[g_j], group_embs[g_i], item_emb, As[g_j], Ws)
                value_list.append(prob_ij)
                index_list.append([g_i, g_j])
                value_list.append(prob_ji)
                index_list.append([g_j, g_i])
        if g_i > 200:
            break

    i = torch.LongTensor(index_list)
    d = torch.FloatTensor(value_list)
    # print(i.shape, d.shape)
    prob_graph = torch.sparse.FloatTensor(i.t(), d, torch.Size([group_num, group_num]))
    print('Graph Construction Complete')
    return prob_graph


def simulate_dynamic(PG_gg, seedNode):
    newActive = True
    currentActiveNodes = copy.deepcopy(seedNode)
    newActiveNodes = set()
    activatedNodes = copy.deepcopy(seedNode)  # Biar ga keaktivasi 2 kali
    influenceSpread = len(seedNode)

    while newActive:
        for g_i in currentActiveNodes:
            neighbors = PG_gg[g_i].to_dense().data.numpy()
            neighbors = np.where(neighbors > 0)
            for g_j in neighbors[0]:
                if g_j not in activatedNodes:
                    flip_coin = random.random()
                    # flip_coin = 0.8
                    if PG_gg[g_i][g_j] > flip_coin:
                        newActiveNodes.add(g_j)
                        activatedNodes.append(g_j)

        influenceSpread += len(newActiveNodes)
        if newActiveNodes:
            currentActiveNodes = list(newActiveNodes)
            newActiveNodes = set()
        else:
            newActive = False
    # print("activatedNodes",len(activatedNodes),activatedNodes)
    return influenceSpread


def initialize(TOPK):
    # conf == yelp
    LR = 1e0
    Lamda = 1e-6
    Alpha_r = 0 # 1e-6
    L = 0
    # movie == conf
    # LR = 1e-2
    # Lamda =  1e-6
    # Alpha_r = 1e-14
    # file_path = 'E:\\IGR_models\\model_movie_ncf.t7'
    # file_path = 'E:\\IGR_models\\model_movie.t7'
    # file_path = 'E:\\IGR_models\\model_movie_pure.t7'
    # file_path = 'E:\\IGR_models\\model_yelp_ncf.t7'
    # file_path = 'E:\\IGR_models\\model_yelp_prue.t7'
    # file_path = 'E:\\IGR_models\\model_yelp.t7'
    file_path = 'E:\\IGR_models\\model_mfw.t7'
    file_path = 'E:\\IGR_models\\model_mfw_pure.t7'
    file_path = 'E:\\IGR_models\\model_mfw_ncf.t7'
    # GCN_model = ms.init_model_movie(1,2)
    # GCN_model = ms.init_model_yelp(1, L)
    GCN_model = ms.init_model_mfw(1, L)
    state = torch.load(file_path)
    GCN_model = state['model']
    val_item_index, val_group_index = ult.generate_val_data(100)
    GCN_model.eval()
    List_rec_res = []
    List_score = []
    List_label = []
    R_gi = GCN_model.consumed_items_sparse_matrix
    Activeness = torch.sparse.sum(R_gi, dim=1)
    Activeness = Activeness.to_dense()
    Activeness = Activeness.data.numpy()
    mean_a = np.mean(Activeness)
    A_norm = np.where((Activeness > mean_a), np.ones_like(Activeness), Activeness / mean_a)

    for val_item_id in val_item_index:
        val_list = np.ones_like(val_group_index) * val_item_id
        Rs_pos_val, Rs_2_pos_val = GCN_model(val_group_index, val_list)
        scores = (Rs_pos_val + Alpha_r * Rs_2_pos_val).data.numpy()
        labels = GCN_model.generate_labels(val_group_index, val_list)
        des_order = np.argsort(-scores)
        rec_list = val_group_index[des_order[:TOPK]]
        # print(rec_list)
        # List_score.append(scores)
        # List_label.append(labels)
        List_rec_res.append(rec_list)
    return List_rec_res, val_item_index, GCN_model, A_norm


if __name__ == '__main__':
    ans = []
    W = [[0, 1], [0.1, 0.9], [0.2, 0.8], [0.3, 0.7], [0.4, 0.6], [0.5, 0.5], [0.6, 0.4], [0.7, 0.3], [0.8, 0.2],
         [0.9, 0.1], [1, 0]]
    W = [[0, 1], [0.0, 0.9], [0.0, 0.8], [0.0, 0.7], [0., 0.6], [0., 0.5], [0., 0.4], [0., 0.3], [0., 0.2],
         [0., 0.1], [0, 0]]
    W = [[0.1, 0.9], [0.1, 0.8], [0.1, 0.7], [0.1, 0.6], [0.1, 0.5], [0.1, 0.4], [0.1, 0.3], [0.1, 0.2],
         [0.1, 0.1], [0.1, 0]]
    W = []
    for i in range(11):
        for j in range(11-i):
            tmp = [i*0.1, j*0.1]
            print(tmp)
            W.append(tmp)
        print('------------')
    for ele in [5, 10, 20, 30]:
        List_Rec_Res, List_item, model, A_norm = initialize(ele)
        maxx_v = -1
        maxx_id = -1
        W= [[0.1, 0.6]]
        for Ws in W:
            st = time.time()
            test_can = []
            for i in range(4):
                group_influence = DIC(List_Rec_Res[i], List_item[i], model, A_norm, W[0])
                print('rate:', group_influence / A_norm.shape[0])
                test_can.append(group_influence)
            aver = np.mean(np.array(test_can)) / A_norm.shape[0]
            print('The final aver: ', aver)
            if aver > maxx_v:
                maxx_v = aver
                maxx_id = Ws
            ans.append(aver)
            print('-----------------------')
            mins = (time.time() - st) / 60
            print(mins)
            print(ans)
            # break
        print(maxx_v, maxx_id)
    # ans = [0.40678391959798993, 0.4077889447236181, 0.4062814070351759, 0.4062814070351759, 0.40703517587939697, 0.40703517587939697, 0.407286432160804, 0.40678391959798993, 0.407286432160804, 0.40703517587939697, 0.4065326633165829, 0.407286432160804, 0.40753768844221105, 0.4077889447236181, 0.4057788944723618, 0.40753768844221105, 0.407286432160804, 0.4085427135678392, 0.4065326633165829, 0.40703517587939697, 0.40753768844221105, 0.40703517587939697, 0.40678391959798993, 0.40703517587939697, 0.4082914572864322, 0.4077889447236181, 0.40753768844221105, 0.407286432160804, 0.40678391959798993, 0.407286432160804, 0.4082914572864322, 0.40753768844221105, 0.4057788944723618, 0.4082914572864322, 0.40753768844221105, 0.4065326633165829, 0.4062814070351759, 0.4077889447236181, 0.40603015075376886, 0.40703517587939697, 0.4062814070351759, 0.40678391959798993, 0.40527638190954773, 0.40678391959798993, 0.4062814070351759, 0.4065326633165829, 0.40678391959798993, 0.40753768844221105, 0.407286432160804, 0.4057788944723618, 0.40703517587939697, 0.40804020100502514, 0.4077889447236181, 0.40703517587939697, 0.40527638190954773, 0.40678391959798993, 0.407286432160804, 0.40804020100502514, 0.40703517587939697, 0.4062814070351759, 0.4062814070351759, 0.407286432160804, 0.4065326633165829, 0.4065326633165829, 0.4065326633165829]
    # st = 0
    # ed = 0
    # count = 0
    # print('num', len(ans))
    # for i in range(11):
    #     list_t = []
    #     for j in range(11-i):
    #         # tmp = [i*0.1, j*0.1]
    #         # print(tmp)
    #         # W.append(tmp)
    #         np.set_printoptions(precision=4)
    #         print(np.float32(ans[count]), end=' ')
    #         list_t.append(ans[count])
    #         count += 1
    #     print(' ')
    #     print('max:', np.max(list_t))
    # print(ans[count])