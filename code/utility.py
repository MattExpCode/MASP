import math
import matplotlib.pyplot as plt
import torch
from random import *
from pandas import *
import numpy as np

# np.random.seed(2)
torch.manual_seed(2)
torch.cuda.manual_seed_all(2)


class IndexStruction():
    def __init__(self):
        self.hash_family_Para = ''
        self.get_z_para = ''
        self.List_Z = ''

    def init(self, conf):
        self.hash_family_Para = ''
        self.get_z_para = ''
        self.List_Z = ''

    def LookUp(self, Z_value):
        count_ = 256
        L_count = 0
        R_count = 0
        centr = Z_value
        list_pos = [centr]
        for i in range(1, 100):
            L_pos = centr - i
            R_pos = centr + i
            if L_pos < 0:
                continue
            else:
                list_pos.append(L_pos)
            if R_pos > len(self.List_Z):
                continue
            else:
                list_pos.append(R_pos)
        local_pos = self.List_Z[Z_value]

    def hash_famuly(self, feat):
        self.hash_family_Para

    def HashF(self, feat):
        projected_feat = self.hash_family(feat)
        return projected_feat


# def fast_map(k):
#     m = 10;
#     n = 3;
#     X = rand(m, n);
#     [Y, ax] = fastmap(X);
#
#     % % 作图
#     hPoints = plot3(X(:, 1), X(:, 2), X(:, 3), 'r.', 'MarkerSize', 10); % 数据点
#     xlabel('x')
#     ylabel('y')
#     zlabel('z')
#     box
#     on
#     hold
#     on
#     for i = 1:m
#         plot3([0 X(i, 1)], [X(i, 2) X(i, 2)], [X(i, 3) X(i, 3)], 'k:')
#         plot3([X(i, 1) X(i, 1)], [0 X(i, 2)], [X(i, 3) X(i, 3)], 'k:')
#         plot3([X(i, 1) X(i, 1)], [X(i, 2) X(i, 2)], [0 X(i, 3)], 'k:')
#     end
#     plot3(ax(:, 1), ax(:, 2), ax(:, 3), 'b-') % 画轴线
#
#     % % FastMap函数
#     function[Y, axisPoints] = fastmap(X)
#
#     D = squareform(pdist(X, 'Euclidean')); % 欧式距离矩阵
#     [~, ind] = max(D(:)); % 求最大距离
#     [a, b] = ind2sub(size(D), ind); % 最大距离的两个点，即轴对象
#     xa = X(a,:);
#     xb = X(b,:);
#     # axisPoints = [xa;    xb]; % 轴对象坐标，只是为了作图用
#
#     A = null(xa - xb);
#     Y = X * A;
#
#     end


def fastmap(emb):
    X = []
    dist = []
    col = 0

    def distance(a, b, col):
        if (col == 1):
            return dist[a][b]
        else:
            res = (math.pow(distance(a, b, col - 1), 2) - (math.pow(X[a][col - 1] - X[b][col - 1], 2)))
            return math.sqrt(res)

    def chooseDistantObject(col):
        b = randint(1, 10)
        #   start with any random value
        count = 18
        g_max = 0
        g_a = 0
        g_b = 0

        while (count > 0):
            max_dist = 0
            for i in range(1, 11):
                d = distance(b, i, col)
                if (d > max_dist):
                    max_dist = d
                    a = i

            if (max_dist > g_max):
                g_max = max_dist
                g_a = a
                g_b = b
            elif (max_dist == g_max):
                # compare ids
                g_min = min(g_a, g_b)
                c_min = min(a, b)
                if (c_min == min(g_min, c_min)):
                    g_a = a
                    g_b = b

            b = a
            count -= 1;

        # Make point with lower id value as origin
        if (g_b < g_a):
            temp = g_a;
            g_a = g_b;
            g_b = temp;
        return g_a, g_b

    # FastMap implementation
    def FastMap(k):
        global col
        global X
        if k <= 0:
            return
        else:
            col += 1

        a, b = chooseDistantObject(col)

        if distance(a, b, col) == 0:
            for i in range(1, 11):
                X[i][col] = 0
            return

        for i in range(1, 11):
            # calculate projection on line a|<----------------------------------->|b
            if (i == a):
                dim_curr = 0
            elif (i == b):
                dim_curr = distance(a, b, col)
            else:
                dim_curr = (math.pow(distance(a, i, col), 2) + math.pow(distance(a, b, col), 2)
                            - math.pow(distance(b, i, col), 2)) / (2 * distance(a, b, col))

            X[i][col] = dim_curr

        FastMap(k - 1)  # Call FastMap again with updated distances and k-1 dimensions

    # Execution
    f = open('fastmap-data.txt')
    line = f.readline()

    ##initialize 2-D arrays
    dist = [[0 for i in range(11)] for j in range(11)]
    X = [[0 for i in range(3)] for j in range(11)]  # first col and row of x are just created to index properly
    print(dist)
    print(X)

    while line:
        arr = line.split()
        dist[int(arr[0])][int(arr[1])] = int(arr[2])
        dist[int(arr[1])][int(arr[0])] = int(arr[2])
        line = f.readline()
    f.close()

    words = []
    f = open('fastmap-wordlist.txt')
    line = f.readline()
    ##initialize 2-D arrays
    while line:
        words.append(line.splitlines())
        line = f.readline()
    f.close()
    FastMap(2)  # call FastMap

    X = [X[i][1:] for i in range(1, 11)]  ##strip first column
    print(DataFrame(X))

    # Plot the points
    fig, ax = plt.subplots()
    for i in range(10):
        ax.scatter(X[i][0], X[i][1])
        ax.annotate(words[i], (X[i][0], X[i][1]))

    plt.show()


# def UI_Index(item_emb):
#     fastmap(item_emb)
#     H_item_emb = HashF(new_item_emb)
#     Z_item_emb = getZ(H_item_emb)
#     group_ids = LookUp(Z_item_emb)


# def fastmap()
def load_group_emb(filename):
    state = torch.load(filename)
    model = state['model']
    neighbor_embedding = torch.spmm(model.social_neighbors_sparse_matrix, model.final_group_embedding)

    return model.final_group_embedding, neighbor_embedding, model.final_item_embedding


def dist_func(a, b):
    # print(a.shape, b.shape)
    cos = torch.nn.CosineSimilarity(dim=0, eps=1e-08)
    sim = cos(a, b)
    # print(sim)
    return 1 - sim


def compute_r(a_emb, b_emb, a_neighbor_embed):
    Alpha_r = 1e-14
    inner_pro = torch.mul(a_emb, b_emb)
    similarity = torch.mul(a_neighbor_embed, a_emb)
    willingness = torch.mul(a_neighbor_embed, b_emb)
    # print(inner_pro.shape)
    # print(similarity.shape)
    # print(willingness.shape)

    gamma_2 = torch.sum(similarity, dim=0) + torch.sum(willingness, dim=0)
    gamma = torch.sum(inner_pro, dim=0)

    score = (gamma + Alpha_r * gamma_2)
    return score


def FASTMAP(dbname, k):
    # dist_id = np.loadtxt('movie_group_emb_dist.txt')
    # dist_id = np.loadtxt('YELP_group_emb_dist.txt')
    file = dbname + '_group_emb_dist.txt'
    dist_id = np.loadtxt(file)
    # YELP_group_emb_dist
    FIleList = []
    print(dist_id)
    print(dist_id.shape)
    K = k
    # file_path = 'E:\\IGR_models\\model_movie.t7'
    # file_path = 'E:\\IGR_models\\model_YELP_.t7'
    file_path = 'E:\\IGR_models\\model_' + dbname + '.t7'

    g_emb, neb_emb, _ = load_group_emb(file_path)
    group_num = g_emb.shape[0]
    max_coor = 0
    min_coor = 30
    for i in range(group_num):
        gi_emb = g_emb[i]
        tmp_list = [i + 1]
        for x_i in range(K):
            sampling_id = int(np.floor(np.random.rand() * dist_id.shape[0]))
            g_a = int(sampling_id)
            g_b = int(dist_id[sampling_id])
            # print(g_a, g_b)
            a_emb = g_emb[g_a]
            b_emb = g_emb[g_b]
            a_n_emb = neb_emb[g_a]
            b_n_emb = neb_emb[g_b]

            r_ai = compute_r(a_emb, gi_emb, a_n_emb)
            r_ab = compute_r(a_emb, b_emb, a_n_emb)
            r_bi = compute_r(b_emb, gi_emb, b_n_emb)

            new_v = torch.pow(r_ai, 2) + torch.pow(r_ab, 2) - torch.pow(r_bi, 2)
            new_v = new_v / (2 * r_ab)
            x = new_v.data.numpy()
            tmp_list.append(x)
        tmp_list = np.array(tmp_list)
        if np.max(tmp_list[1:]) > max_coor:
            max_coor = np.max(tmp_list[1:])
            # print(min_coor, max_coor)
        if np.min(tmp_list[1:]) < min_coor:
            min_coor = np.min(tmp_list[1:])
            # print(min_coor, max_coor)

        FIleList.append(tmp_list)
        # break
    print(min_coor, max_coor)
    mininum = np.floor(min_coor)
    FIleList = np.array(FIleList)
    print(FIleList.shape)
    FIleList[:, 1:] = FIleList[:, 1:] - mininum
    save_file = './UI_Index/Projected_group_Embedding_' + dbname + '_' + str(k) + '_new.txt'
    # np.savetxt('Projected_group_Embedding_Movie.txt', FIleList)
    # np.savetxt('Projected_group_Embedding_Yelp.txt', FIleList)
    np.savetxt(save_file, FIleList)


def FASTMAP_item(dbname, k):
    # dist_id = np.loadtxt('movie_group_emb_dist.txt')
    # dist_id = np.loadtxt('YELP_group_emb_dist.txt')
    file = dbname + '_group_emb_dist.txt'
    dist_id = np.loadtxt(file)
    FIleList = []
    print(dist_id)
    print(dist_id.shape)
    K = k
    # file_path = 'E:\\IGR_models\\model_movie.t7'
    # file_path = 'E:\\IGR_models\\model_YELP_.t7'

    file_path = 'E:\\IGR_models\\model_' + dbname + '.t7'

    g_emb, neb_emb, i_emb = load_group_emb(file_path)
    group_num = g_emb.shape[0]
    item_num = i_emb.shape[0]
    max_coor = 0
    min_coor = 30
    for i in range(item_num):
        ii_emb = i_emb[i]
        tmp_list = [i + 1]
        for x_i in range(K):
            sampling_id = int(np.floor(np.random.rand() * dist_id.shape[0]))
            g_a = int(sampling_id)
            g_b = int(dist_id[sampling_id])
            # print(g_a, g_b)
            a_emb = g_emb[g_a]
            b_emb = g_emb[g_b]
            a_n_emb = neb_emb[g_a]
            b_n_emb = neb_emb[g_b]

            r_ai = compute_r(a_emb, ii_emb, a_n_emb)
            r_ab = compute_r(a_emb, b_emb, a_n_emb)
            r_bi = compute_r(b_emb, ii_emb, b_n_emb)

            new_v = torch.pow(r_ai, 2) + torch.pow(r_ab, 2) - torch.pow(r_bi, 2)
            new_v = new_v / (2 * r_ab)
            x = new_v.data.numpy()
            tmp_list.append(x)
        tmp_list = np.array(tmp_list)
        if np.max(tmp_list[1:]) > max_coor:
            max_coor = np.max(tmp_list[1:])
            # print(min_coor, max_coor)
        if np.min(tmp_list[1:]) < min_coor:
            min_coor = np.min(tmp_list[1:])
            # print(min_coor, max_coor)

        FIleList.append(tmp_list)
        # break
    print(min_coor, max_coor)
    mininum = np.floor(min_coor)
    FIleList = np.array(FIleList)
    print(FIleList.shape)
    FIleList[:, 1:] = FIleList[:, 1:] - min_coor
    save_file = './UI_Index/Projected_item_Embedding_' + dbname + '_' + str(k) + '_new.txt'
    # np.savetxt('Projected_item_Embedding_Movie.txt', FIleList)
    # np.savetxt('Projected_item_Embedding_Yelp.txt', FIleList)
    np.savetxt(save_file, FIleList)


def generate_max_dist():
    file_path = 'E:\\IGR_models\\model_movie.t7'
    # file_path = 'E:\\IGR_models\\model_YELP_.t7'
    # 
    g_emb, _, _ = load_group_emb(file_path)
    group_num = g_emb.shape[0]
    List_ = []
    print(g_emb.shape)

    for i in range(group_num):
        emb_i = g_emb[i]
        maxx = 0
        max_id = -1
        print('deal', i)
        for j in range(group_num):
            emb_j = g_emb[j]
            dis = dist_func(emb_i, emb_j)
            if maxx < dis:
                maxx = dis
                max_id = j
        List_.append(max_id)
    List_ = np.array(List_)
    np.savetxt('movie_group_emb_dist.txt', List_)
    # np.savetxt('YELP_group_emb_dist.txt', List_)
    print(List_.shape)
    return 0


if __name__ == '__main__':
    # generate_max_dist()
    # 'movie' 'YELP'
    dbnames = ['movie', 'YELP']
    # FASTMAP(dbnames[0], 10)
    # FASTMAP_item(dbnames[0], 10)
