# -- coding: utf-8 --
import numpy as np
import torch
import torch.nn as nn
import dgl
from dgl.data import CoraGraphDataset
import random
import Models as ms
import utilize as utl
from sklearn.metrics import roc_curve, auc, precision_recall_curve, recall_score, f1_score, precision_score
import time

random.seed(2)
# np.random.seed(2)
torch.manual_seed(2)
torch.cuda.manual_seed_all(2)


def select_topK(data_, label_, topK):
    tmp_arr_ = np.sort(data_)
    new_data_ = np.where((data_ > tmp_arr_[-topK]), np.ones_like(data_), np.zeros_like(data_))
    hr_ = utl.HR(label_, new_data_)
    ndcg_ = utl.compute_NDCG(label_, new_data_)
    print('hr & ndcg:', hr_, ndcg_)
    return hr_, ndcg_


def cmp_func(config, dataname):
    List_label = []
    List_score = []
    K_list = [5, 10, 20, 30]
    all_neighbors = torch.spmm(GCN_model.social_neighbors_sparse_matrix, all_groups)
    hashD, sortD, Dindex = utl.build_UI_Index(dataname)
    if config == 0:
        Batches = utl.generate_batches_index(dataname, hashD, sortD, Dindex, 1000)
    else:
        Batches = utl.generate_batches_index_LSH(dataname, hashD, sortD, Dindex, 1000)
    print(len(Batches))

    st = time.time()
    for batch in Batches:
        group_id_index = batch[0]
        item_id_index = batch[1]
        groups_emb = all_groups[group_id_index]
        items_emb = all_items[item_id_index]
        groups_neighbors_emb = all_neighbors[group_id_index]
        inner_pro = torch.mul(groups_emb, items_emb)
        similarity = torch.mul(groups_neighbors_emb, groups_emb)
        willingness = torch.mul(groups_neighbors_emb, items_emb)
        gamma_2 = torch.sum(similarity, dim=1) + torch.sum(willingness, dim=1)
        gamma = torch.sum(inner_pro, dim=1)
        scores = (gamma + Alpha_r * gamma_2).data.numpy()
        labels = GCN_model.generate_labels(group_id_index, item_id_index)
        List_score.append(scores)
        List_label.append(labels)
        # print(np.max(scores))
    secs = (time.time() - st)
    print('indexed: ', secs)
    hr_l = []
    ndcg_l = []
    for topk in K_list:
        # if topk != 50:
        #     continue
        hr, ndcg = utl.Evaluation(List_score, List_label, topk)
        # print('hr & ndcg:', hr, ndcg)
        hr_l.append(hr)
        ndcg_l.append(ndcg)
    print('hr', hr_l)
    print('ndcg', ndcg_l)


def cmp_func_ori(config):
    if config == 0:
        val_group_index = np.arange(0, 1350, 1)

    else:
        if config == 1:
            val_group_index = np.arange(0, 24103, 1)
        else:
            val_group_index = np.arange(0, 995, 1)
            val_group_index = np.hstack([val_group_index,val_group_index[:360]])

    print('!!!!!!!!!', val_group_index.shape)
    Batches = utl.gene_batch_array(val_group_index)

    List_label = []
    List_score = []
    K_list = [5, 10, 20, 30]

    st = time.time()
    all_neighbors = torch.spmm(GCN_model.social_neighbors_sparse_matrix, all_groups)
    for item_N in range(1000):
        print(item_N, len(Batches))
        for batch in Batches:
            groups_emb = all_groups[batch]
            val_list = np.ones_like(batch)
            items_emb = all_items[val_list * item_N]
            groups_neighbors_emb = all_neighbors[batch]
            inner_pro = torch.mul(groups_emb, items_emb)
            similarity = torch.mul(groups_neighbors_emb, groups_emb)
            willingness = torch.mul(groups_neighbors_emb, items_emb)
            gamma_2 = torch.sum(similarity, dim=1) + torch.sum(willingness, dim=1)
            gamma = torch.sum(inner_pro, dim=1)
            scores = (gamma + Alpha_r * gamma_2).data.numpy()
            labels = GCN_model.generate_labels(batch, val_list * item_N)
            List_score.append(scores)
            List_label.append(labels)
        # print(gamma.shape, gamma_2.shape)
    secs = (time.time() - st)
    print('original ', secs)

    hr_l = []
    ndcg_l = []
    for topk in K_list:
        # if topk != 50:
        #     continue
        hr, ndcg = utl.Evaluation(List_score, List_label, topk)
        # print('hr & ndcg:', hr, ndcg)
        hr_l.append(hr)
        ndcg_l.append(ndcg)
    print('hr', hr_l)
    print('ndcg', ndcg_l)


if __name__ == '__main__':

    List = [[1, 2], [2, 3], [3, 4], [4, 5]]
    List = np.array(List)
    id = np.array([3, 0])
    print(List[id, 0])

    FINE_TUNE = 0
    TRAIN = 0
    SAVE = 0

    TEST = 1
    EVAL = 0
    TIME_CMP = 1
    MOVIE = 0
    YELP = 0
    MFW = 1

    if FINE_TUNE:
        LR = 1e-1
        Lamda = 1e-5
        Alpha_r = 1e-6
        L = 2
        # conf == yelp
        # LR = 1e0
        # Lamda = 1e-6
        # Alpha_r = 1e-6

        # movie == conf
        # LR = 1e-2
        # Lamda =  1e-6
        # Alpha_r = 1e-14

        pre_def = 1
        SavePath = 'E:\\IGR_models\\'
        # GCN_model = ms.init_model_large(pre_def)
        # GCN_model = ms.init_model_yelp(pre_def)
        # GCN_model = ms.init_model_movie(1)
        # GCN_model = ms.init_model_YELP(pre_def)

        if MOVIE == 1:
            GCN_model = ms.init_model_movie(pre_def, L)
        if YELP == 1:
            GCN_model = ms.init_model_YELP(pre_def, L)
        if MFW == 1:
            GCN_model = ms.init_model_mfw(pre_def, L)

        GCN_model.train()
        embed_g = GCN_model.embedding_group.weight
        embed_i = GCN_model.embedding_item.weight
        print(embed_g.shape)
        print(embed_i.shape)
        train_index = utl.generate_train_data()
        val_item_index, val_group_index = utl.generate_val_data(100)

        rand_id = utl.random_id(train_index)
        print('edges_num: ', len(train_index))
        edges_num = len(train_index)
        dropped_edges = int(np.floor(edges_num * 0.9))
        rand_idex = rand_id[:dropped_edges]
        Batches = utl.gene_batch(train_index[rand_idex])
        LR_1_list = [1e-4, 1e-3, 1e-2, 1e-1, 1e-0]
        LR_1_list = [1e-2, 1e-1, 1e-0]
        Lamada_1_list = [1e-4, 1e-5, 1e-6, 1e-7, 1e-8]
        Lamada_1_list = [1e-5]
        Alpha_r_list = [1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8]
        Alpha_r_list = [1e-6]
        L_list = [0, 1, 2, 3, 4]
        L_list = [2]
        for LR in LR_1_list:
            if MOVIE == 1:
                GCN_model = ms.init_model_movie(pre_def, L)
            if YELP == 1:
                GCN_model = ms.init_model_YELP(pre_def, L)
            if MFW == 1:
                GCN_model = ms.init_model_mfw(pre_def, L)
            GCN_model.train()
            print('Lamda=', Lamda)
            print('LR=', LR)
            print('Alpha_r=', Alpha_r)
            print('L=', L)

            ANS = []
            optimizer = torch.optim.Adam(GCN_model.parameters(), lr=LR, weight_decay=Lamda)
            epoch = 201
            # GCN_model()
            count_hr = 0
            count_ndcg = 0
            hrs = [-1, -1]
            ndcgs = [-1, -1]
            TIME = []
            TIME_IN = []
            for ep in range(epoch):
                GCN_model.train()
                batch_num = len(Batches)
                tmp_ = np.arange(0, batch_num, 1)
                # np.random.shuffle(tmp_)
                drop_out = int(np.floor(batch_num * 0.8))
                for b_id in range(drop_out):
                    batch_id = tmp_[b_id]
                    batch = Batches[batch_id]
                    group_ids = batch[:, 0]
                    pos_item_ids = batch[:, 1]
                    neg_item_ids = batch[:, 2]
                    Rs_pos, Rs_2_pos = GCN_model(group_ids, pos_item_ids)
                    Rs_neg, Rs_2_neg = GCN_model(group_ids, neg_item_ids)
                    Rs_pos += Alpha_r * Rs_2_pos
                    Rs_neg += Alpha_r * Rs_2_neg
                    optimizer.zero_grad()
                    loss = -torch.mean(torch.sigmoid(torch.nn.functional.softplus(Rs_pos - Rs_neg)))
                    # loss_2 = -torch.mean(torch.sigmoid(torch.nn.functional.softplus(Rs_2_pos - Rs_2_neg)))
                    # loss = -(Rs_pos - Rs_neg).sigmoid().log().mean()
                    # print(ep, loss.data.numpy())
                    # loss += loss_2*Alpha_r
                    loss.backward()
                    optimizer.step()
                print('Epoch ', ep, loss.data.numpy())
                if ep % 5 == 1:
                    print('----------------Epoch ', ep, loss.data.numpy())
                    GCN_model.eval()
                    List_score = []
                    List_label = []
                    st = time.time()
                    # print('val item index', val_item_index)
                    # print('val grou index', val_group_index)

                    for val_item_id in val_item_index:
                        val_list = np.ones_like(val_group_index) * val_item_id
                        st_1 = time.time()
                        Rs_pos_val, Rs_2_pos_val = GCN_model(val_group_index, val_list)
                        time_cost_inner = time.time() - st_1
                        TIME_IN.append(time_cost_inner)
                        scores = (Rs_pos_val + Alpha_r * Rs_2_pos_val).data.numpy()
                        # print('scores: ', scores.shape)
                        # print(val_group_index.shape)
                        # print()
                        labels = GCN_model.generate_labels(val_group_index, val_list)
                        List_score.append(scores)
                        List_label.append(labels)
                    K_list = [5, 10, 20, 30, 50]
                    time_cost = time.time() - st
                    TIME.append(time_cost)
                    for topk in K_list:
                        if topk != 20:
                            continue
                        # if topk > 20:
                        #     continue
                        hr, ndcg = utl.Evaluation(List_score, List_label, topk)
                        print('hr & ndcg:', hr, ndcg)
                        ANS.append([hr, ndcg])
                        # break

                    if hr > hrs[0]:
                        hrs[1] = hrs[0]
                        # print(hrs)
                        hrs[0] = hr
                        # print(hrs)
                        count_hr = 0
                    elif hr > hrs[1]:
                        hrs[1] = hr
                        count_hr = 0
                    else:
                        count_hr += 1
                    if ndcg > ndcgs[0]:
                        ndcgs[1] = ndcgs[0]
                        ndcgs[0] = ndcg
                        count_ndcg = 0
                    elif ndcg > ndcgs[1]:
                        ndcgs[1] = ndcg
                        count_ndcg = 0
                    else:
                        count_ndcg += 1
                    # print(hrs)
                    # print(ndcgs)
                    print(count_hr, count_ndcg)
                    if count_hr > 3 and count_ndcg > 3:
                        print('Early Stop!')
                        break
                    # # p, r, t = precision_recall_curve(label, data)
                    # # print('recall:', recall, precision, ff1)
                    # maxx = -1
                    # posi = 0
                    # ii = 0
                    # for x, y in (zip(p, r)):
                    #     if x == 0 or y == 0:
                    #         continue
                    #     f1 = 2 * x * y / (x + y)
                    #     if maxx < f1:
                    #         maxx = f1
                    #         posi = ii
                    #     ii += 1
                    # print(p[posi], r[posi], maxx)

                # print('val loss:', pos_value.data.numpy(), neg_value.data.numpy(),
                #       pos_value.data.numpy() + neg_value.data.numpy())
                # print('aver', aver_score_pos, aver_score_neg)
            state = {'model': GCN_model, 'optimizer': optimizer}
            TIME = np.array(TIME)
            TIME_IN = np.array(TIME_IN)
            # print('aver time: ', TIME.mean(), TIME_IN.mean())
            if SAVE:
                file_name = SavePath + 'model_mfw.t7'
                print(file_name)
                torch.save(state, file_name)
            print(ANS)
            del GCN_model
    if TRAIN:
        LR = 1e-1
        Lamda = 1e-5
        Alpha_r = 1e-6
        L = 2

        # conf == mfw
        # LR = 1e-1
        # Lamda = 1e-5
        # Alpha_r = 1e-6
        # L = 2

        # conf == yelp
        # LR = 1e0
        # Lamda = 1e-6
        # Alpha_r = 1e-6

        # movie == conf
        # LR = 1e-2
        # Lamda =  1e-6
        # Alpha_r = 1e-14

        pre_def = 1
        SavePath = 'E:\\IGR_models\\'
        # GCN_model = ms.init_model_large(pre_def)
        # GCN_model = ms.init_model_yelp(pre_def)
        # GCN_model = ms.init_model_movie(1)
        # GCN_model = ms.init_model_YELP(pre_def)

        if MOVIE == 1:
            GCN_model = ms.init_model_movie(pre_def, L)
        if YELP == 1:
            GCN_model = ms.init_model_YELP(pre_def, L)
        if MFW == 1:
            GCN_model = ms.init_model_mfw(pre_def, L)

        GCN_model.train()
        optimizer = torch.optim.Adam(GCN_model.parameters(), lr=LR, weight_decay=Lamda)
        embed_g = GCN_model.embedding_group.weight
        embed_i = GCN_model.embedding_item.weight
        print(embed_g.shape)
        print(embed_i.shape)
        train_index = utl.generate_train_data()
        val_item_index, val_group_index = utl.generate_val_data(100)
        rand_id = utl.random_id(train_index)
        print('edges_num: ', len(train_index))
        edges_num = len(train_index)
        dropped_edges = int(np.floor(edges_num * 0.9))
        rand_idex = rand_id[:dropped_edges]
        Batches = utl.gene_batch(train_index[rand_idex])

        epoch = 201
        # GCN_model()
        count_hr = 0
        count_ndcg = 0
        hrs = [-1, -1]
        ndcgs = [-1, -1]
        TIME = []
        TIME_IN = []
        for ep in range(epoch):
            GCN_model.train()
            # rand_idex = rand_id[:90000]
            # group_ids = train_index[rand_idex, 0]
            # pos_item_ids = train_index[rand_idex, 1]
            # neg_item_ids = train_index[rand_idex, 2]
            # print(group_ids, group_ids.shape)
            # print(pos_item_ids, pos_item_ids.shape)
            # print(neg_item_ids, neg_item_ids.shape)
            batch_num = len(Batches)
            tmp_ = np.arange(0, batch_num, 1)
            # np.random.shuffle(tmp_)
            drop_out = int(np.floor(batch_num * 0.8))
            for b_id in range(drop_out):
                batch_id = tmp_[b_id]
                batch = Batches[batch_id]
                group_ids = batch[:, 0]
                pos_item_ids = batch[:, 1]
                neg_item_ids = batch[:, 2]
                Rs_pos, Rs_2_pos = GCN_model(group_ids, pos_item_ids)
                Rs_neg, Rs_2_neg = GCN_model(group_ids, neg_item_ids)
                Rs_pos += Alpha_r * Rs_2_pos
                Rs_neg += Alpha_r * Rs_2_neg
                optimizer.zero_grad()
                loss = -torch.mean(torch.sigmoid(torch.nn.functional.softplus(Rs_pos - Rs_neg)))
                # loss_2 = -torch.mean(torch.sigmoid(torch.nn.functional.softplus(Rs_2_pos - Rs_2_neg)))
                # loss = -(Rs_pos - Rs_neg).sigmoid().log().mean()
                # print(ep, loss.data.numpy())
                # loss += loss_2*Alpha_r
                loss.backward()
                optimizer.step()
            print('Epoch ', ep, loss.data.numpy())
            if ep % 5 == 1:
                print('----------------Epoch ', ep, loss.data.numpy())
                GCN_model.eval()
                List_score = []
                List_label = []
                st = time.time()
                for val_item_id in val_item_index:
                    val_list = np.ones_like(val_group_index) * val_item_id
                    st_1 = time.time()
                    Rs_pos_val, Rs_2_pos_val = GCN_model(val_group_index, val_list)
                    time_cost_inner = time.time() - st_1
                    TIME_IN.append(time_cost_inner)
                    scores = (Rs_pos_val + Alpha_r * Rs_2_pos_val).data.numpy()
                    labels = GCN_model.generate_labels(val_group_index, val_list)
                    List_score.append(scores)
                    List_label.append(labels)
                K_list = [5, 10, 20, 30, 50]
                time_cost = time.time() - st
                TIME.append(time_cost)
                for topk in K_list:
                    # if topk != 20:
                    #     continue
                    hr, ndcg = utl.Evaluation(List_score, List_label, topk)
                    print('hr & ndcg:', hr, ndcg)
                    # break

                if hr > hrs[0]:
                    hrs[1] = hrs[0]
                    # print(hrs)
                    hrs[0] = hr
                    # print(hrs)
                    count_hr = 0
                elif hr > hrs[1]:
                    hrs[1] = hr
                    count_hr = 0
                else:
                    count_hr += 1
                if ndcg > ndcgs[0]:
                    ndcgs[1] = ndcgs[0]
                    ndcgs[0] = ndcg
                    count_ndcg = 0
                elif ndcg > ndcgs[1]:
                    ndcgs[1] = ndcg
                    count_ndcg = 0
                else:
                    count_ndcg += 1
                # print(hrs)
                # print(ndcgs)
                print(count_hr, count_ndcg)
                if count_hr > 3 and count_ndcg > 3:
                    print('Early Stop!')
                    break
                # # p, r, t = precision_recall_curve(label, data)
                # # print('recall:', recall, precision, ff1)
                # maxx = -1
                # posi = 0
                # ii = 0
                # for x, y in (zip(p, r)):
                #     if x == 0 or y == 0:
                #         continue
                #     f1 = 2 * x * y / (x + y)
                #     if maxx < f1:
                #         maxx = f1
                #         posi = ii
                #     ii += 1
                # print(p[posi], r[posi], maxx)

            # print('val loss:', pos_value.data.numpy(), neg_value.data.numpy(),
            #       pos_value.data.numpy() + neg_value.data.numpy())
            # print('aver', aver_score_pos, aver_score_neg)
        state = {'model': GCN_model, 'optimizer': optimizer}
        TIME = np.array(TIME)
        TIME_IN = np.array(TIME_IN)
        print('aver time: ', TIME.mean(), TIME_IN.mean())
        if SAVE:
            file_name = SavePath + 'model_mfw_pure.t7'
            print(file_name)
            torch.save(state, file_name)
    if TEST:
        # conf == mfw
        LR = 1e-1
        Lamda = 1e-5
        Alpha_r =  1e-6
        L = 2 # 2
        # conf == yelp
        # LR = 1e0
        # Lamda = 1e-6
        # Alpha_r = 1e-6
        # L = 2  # 2
        # movie == conf
        # LR = 1e-2
        # Lamda =  1e-6
        # Alpha_r = 1e-14
        # L = 2
        if MOVIE:
            # file_path = 'E:\\IGR_models\\model_movie_ncf.t7'
            file_path = 'E:\\IGR_models\\model_movie.t7'
            # file_path = 'E:\\IGR_models\\model_movie_pure.t7'
            GCN_model = ms.init_model_movie(1, L)
        if YELP:
            # file_path = 'E:\\IGR_models\\model_YELP_ncf.t7'
            # file_path = 'E:\\IGR_models\\model_YELP_prue.t7'
            file_path = 'E:\\IGR_models\\model_YELP.t7'
            GCN_model = ms.init_model_yelp(1, L)
        if MFW:
            file_path = 'E:\\IGR_models\\model_mfw.t7'
            # file_path = 'E:\\IGR_models\\model_mfw_pure.t7'
            # file_path = 'E:\\IGR_models\\model_mfw_ncf.t7'
            GCN_model = ms.init_model_mfw(1, L)
        state = torch.load(file_path)
        GCN_model = state['model']
        val_item_index, val_group_index = utl.generate_val_data(175)
        GCN_model.eval()
        List_score = []
        List_label = []

        # target = GCN_model.final_group_embedding
        # target = target.to_dense().data.numpy()
        # print(target.shape)
        # print(target)
        # arrOid = np.arange(1, 1351).reshape(1350,1)
        # new = np.concatenate((arrOid, target),1)
        # print(new.shape)
        # print(new)
        # np.savetxt('movielens.txt', new)
        if EVAL:
            st = time.time()
            for val_item_id in val_item_index:
                val_list = np.ones_like(val_group_index) * val_item_id
                st_1 = time.time()
                Rs_pos_val, Rs_2_pos_val = GCN_model(val_group_index, val_list)
                time_cost_inner = time.time() - st_1

                scores = (Rs_pos_val + Alpha_r * Rs_2_pos_val).data.numpy()
                labels = GCN_model.generate_labels(val_group_index, val_list)
                print(np.max(scores))
                List_score.append(scores)
                List_label.append(labels)
            K_list = [5, 10, 20, 30]
            time_cost = time.time() - st
            hr_l = []
            ndcg_l = []

            for topk in K_list:
                # if topk != 50:
                #     continue
                hr, ndcg = utl.Evaluation(List_score, List_label, topk)
                # print('hr & ndcg:', hr, ndcg)
                hr_l.append(hr)
                ndcg_l.append(ndcg)
            print('hr', hr_l)
            print('ndcg', ndcg_l)
        if TIME_CMP:
            state = torch.load(file_path)
            GCN_model = state['model']
            all_groups = GCN_model.final_group_embedding
            all_items = GCN_model.final_item_embedding
            print(all_groups.shape)
            print(all_items.shape)

            if MFW:
                cmp_func_ori(2)

            if MOVIE:
                # val_group_index = np.arange(0, 1350, 1)
                # Batches = utl.gene_batch_array(val_group_index)
                # val_list = np.ones_like(val_group_index)
                cmp_func_ori(0)
                # List_label = []
                # List_score = []
                # K_list = [5, 10, 20, 30]
                #
                # st = time.time()
                # all_neighbors = torch.spmm(GCN_model.social_neighbors_sparse_matrix, all_groups)
                # # for item_N in range(1000):
                # #     print(item_N, len(Batches))
                # #     for batch in Batches:
                # #         groups_emb = all_groups[batch]
                # #         val_list = np.ones_like(batch)
                # #         items_emb = all_items[val_list * item_N]
                # #         groups_neighbors_emb = all_neighbors[batch]
                # #         inner_pro = torch.mul(groups_emb, items_emb)
                # #         similarity = torch.mul(groups_neighbors_emb, groups_emb)
                # #         willingness = torch.mul(groups_neighbors_emb, items_emb)
                # #         gamma_2 = torch.sum(similarity, dim=1) + torch.sum(willingness, dim=1)
                # #         gamma = torch.sum(inner_pro, dim=1)
                # #         scores = (gamma + Alpha_r * gamma_2).data.numpy()
                # #         labels = GCN_model.generate_labels(batch, val_list * item_N)
                # #         List_score.append(scores)
                # #         List_label.append(labels)
                # #     # print(gamma.shape, gamma_2.shape)
                # # secs = (time.time() - st)
                # # print('original ', secs)
                # #
                # # hr_l = []
                # # ndcg_l = []
                # # for topk in K_list:
                # #     # if topk != 50:
                # #     #     continue
                # #     hr, ndcg = utl.Evaluation(List_score, List_label, topk)
                # #     # print('hr & ndcg:', hr, ndcg)
                # #     hr_l.append(hr)
                # #     ndcg_l.append(ndcg)
                # # print('hr', hr_l)
                # # print('ndcg', ndcg_l)
                # st = time.time()
                #
                # hashD, sortD, Dindex = utl.build_UI_Index('movie')
                # Batches = utl.generate_batches_index('movie', hashD, sortD, Dindex, 1000)
                # print(len(Batches))
                #
                # List_label = []
                # List_score = []
                #
                # for batch in Batches:
                #     group_id_index = batch[0]
                #     item_id_index = batch[1]
                #     groups_emb = all_groups[group_id_index]
                #     items_emb = all_items[item_id_index]
                #     groups_neighbors_emb = all_neighbors[group_id_index]
                #     inner_pro = torch.mul(groups_emb, items_emb)
                #     similarity = torch.mul(groups_neighbors_emb, groups_emb)
                #     willingness = torch.mul(groups_neighbors_emb, items_emb)
                #     gamma_2 = torch.sum(similarity, dim=1) + torch.sum(willingness, dim=1)
                #     gamma = torch.sum(inner_pro, dim=1)
                #     scores = (gamma + Alpha_r * gamma_2).data.numpy()
                #     labels = GCN_model.generate_labels(group_id_index, item_id_index)
                #     List_score.append(scores)
                #     List_label.append(labels)
                #     # print(np.max(scores))
                # secs = (time.time() - st)
                # print('indexed: ', secs)
                # hr_l = []
                # ndcg_l = []
                # for topk in K_list:
                #     # if topk != 50:
                #     #     continue
                #     hr, ndcg = utl.Evaluation(List_score, List_label, topk)
                #     # print('hr & ndcg:', hr, ndcg)
                #     hr_l.append(hr)
                #     ndcg_l.append(ndcg)
                # print('hr', hr_l)
                # print('ndcg', ndcg_l)
                # st = time.time()
                #
                # hashD, sortD, Dindex = utl.build_UI_Index('movie')
                # Batches = utl.generate_batches_index_LSH('movie', hashD, sortD, Dindex, 1000)
                # print(len(Batches))
                #
                # List_label = []
                # List_score = []
                #
                # # st = time.time()
                # for batch in Batches:
                #     group_id_index = batch[0]
                #     item_id_index = batch[1]
                #     groups_emb = all_groups[group_id_index]
                #     items_emb = all_items[item_id_index]
                #     groups_neighbors_emb = all_neighbors[group_id_index]
                #     inner_pro = torch.mul(groups_emb, items_emb)
                #     similarity = torch.mul(groups_neighbors_emb, groups_emb)
                #     willingness = torch.mul(groups_neighbors_emb, items_emb)
                #     gamma_2 = torch.sum(similarity, dim=1) + torch.sum(willingness, dim=1)
                #     gamma = torch.sum(inner_pro, dim=1)
                #     scores = (gamma + Alpha_r * gamma_2).data.numpy()
                #     labels = GCN_model.generate_labels(group_id_index, item_id_index)
                #     List_score.append(scores)
                #     List_label.append(labels)
                #     # print(np.max(scores))
                # secs = (time.time() - st)
                # print('indexed: ', secs)
                # hr_l = []
                # ndcg_l = []
                # for topk in K_list:
                #     # if topk != 50:
                #     #     continue
                #     hr, ndcg = utl.Evaluation(List_score, List_label, topk)
                #     # print('hr & ndcg:', hr, ndcg)
                #     hr_l.append(hr)
                #     ndcg_l.append(ndcg)
                # print('hr', hr_l)
                # print('ndcg', ndcg_l)

            if YELP:
                # val_group_index = np.arange(0, 24103, 1)
                # Batches = utl.gene_batch_array(val_group_index)
                # val_list = np.ones_like(val_group_index)
                cmp_func(0, 'yelp')
                cmp_func(1, 'yelp')

                # List_label = []
                # List_score = []
                # K_list = [5, 10, 20, 30]
                #
                # # st = time.time()
                # all_neighbors = torch.spmm(GCN_model.social_neighbors_sparse_matrix, all_groups)
                # # for item_N in range(1000):
                # #     print(item_N, len(Batches))
                # #     for batch in Batches:
                # #         groups_emb = all_groups[batch]
                # #         val_list = np.ones_like(batch)
                # #         items_emb = all_items[val_list * item_N]
                # #         groups_neighbors_emb = all_neighbors[batch]
                # #         inner_pro = torch.mul(groups_emb, items_emb)
                # #         similarity = torch.mul(groups_neighbors_emb, groups_emb)
                # #         willingness = torch.mul(groups_neighbors_emb, items_emb)
                # #         gamma_2 = torch.sum(similarity, dim=1) + torch.sum(willingness, dim=1)
                # #         gamma = torch.sum(inner_pro, dim=1)
                # #         scores = (gamma + Alpha_r * gamma_2).data.numpy()
                # #         labels = GCN_model.generate_labels(batch, val_list * item_N)
                # #         List_score.append(scores)
                # #         List_label.append(labels)
                # #     # print(gamma.shape, gamma_2.shape)
                # # secs = (time.time() - st)
                # # print('original ', secs)
                #
                # # hr_l = []
                # # ndcg_l = []
                # # for topk in K_list:
                # #     # if topk != 50:
                # #     #     continue
                # #     hr, ndcg = utl.Evaluation(List_score, List_label, topk)
                # #     # print('hr & ndcg:', hr, ndcg)
                # #     hr_l.append(hr)
                # #     ndcg_l.append(ndcg)
                # # print('hr', hr_l)
                # # print('ndcg', ndcg_l)
                #
                # hashD, sortD, Dindex = utl.build_UI_Index('yelp')
                # Batches = utl.generate_batches_index_LSH('yelp', hashD, sortD, Dindex, 1000)
                # print(len(Batches))
                #
                # List_label = []
                # List_score = []
                #
                # st = time.time()
                # for batch in Batches:
                #     group_id_index = batch[0]
                #     item_id_index = batch[1]
                #     groups_emb = all_groups[group_id_index]
                #     items_emb = all_items[item_id_index]
                #     groups_neighbors_emb = all_neighbors[group_id_index]
                #     inner_pro = torch.mul(groups_emb, items_emb)
                #     similarity = torch.mul(groups_neighbors_emb, groups_emb)
                #     willingness = torch.mul(groups_neighbors_emb, items_emb)
                #     gamma_2 = torch.sum(similarity, dim=1) + torch.sum(willingness, dim=1)
                #     gamma = torch.sum(inner_pro, dim=1)
                #     scores = (gamma + Alpha_r * gamma_2).data.numpy()
                #     labels = GCN_model.generate_labels(group_id_index, item_id_index)
                #     List_score.append(scores)
                #     List_label.append(labels)
                #     # print(np.max(scores))
                # secs = (time.time() - st)
                # print('indexed: ', secs)
                # hr_l = []
                # ndcg_l = []
                # for topk in K_list:
                #     # if topk != 50:
                #     #     continue
                #     hr, ndcg = utl.Evaluation(List_score, List_label, topk)
                #     # print('hr & ndcg:', hr, ndcg)
                #     hr_l.append(hr)
                #     ndcg_l.append(ndcg)
                # print('hr', hr_l)
                # print('ndcg', ndcg_l)
                #
                # hashD, sortD, Dindex = utl.build_UI_Index('yelp')
                # Batches = utl.generate_batches_index('yelp', hashD, sortD, Dindex, 1000)
                # print(len(Batches))
                #
                # List_label = []
                # List_score = []
                #
                # st = time.time()
                # for batch in Batches:
                #     group_id_index = batch[0]
                #     item_id_index = batch[1]
                #     groups_emb = all_groups[group_id_index]
                #     items_emb = all_items[item_id_index]
                #     groups_neighbors_emb = all_neighbors[group_id_index]
                #     inner_pro = torch.mul(groups_emb, items_emb)
                #     similarity = torch.mul(groups_neighbors_emb, groups_emb)
                #     willingness = torch.mul(groups_neighbors_emb, items_emb)
                #     gamma_2 = torch.sum(similarity, dim=1) + torch.sum(willingness, dim=1)
                #     gamma = torch.sum(inner_pro, dim=1)
                #     scores = (gamma + Alpha_r * gamma_2).data.numpy()
                #     labels = GCN_model.generate_labels(group_id_index, item_id_index)
                #     List_score.append(scores)
                #     List_label.append(labels)
                #     # print(np.max(scores))
                # secs = (time.time() - st)
                # print('indexed: ', secs)
                # hr_l = []
                # ndcg_l = []
                # for topk in K_list:
                #     # if topk != 50:
                #     #     continue
                #     hr, ndcg = utl.Evaluation(List_score, List_label, topk)
                #     # print('hr & ndcg:', hr, ndcg)
                #     hr_l.append(hr)
                #     ndcg_l.append(ndcg)
                # print('hr', hr_l)
                # print('ndcg', ndcg_l)
