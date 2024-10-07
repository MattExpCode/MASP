# -- coding: utf-8 --
import torch
import torch.nn as nn
import numpy as np
from torch.autograd import Variable
import torch.nn.functional as F


def graph_construction():
    graph = [[0, 1, 1, 1], [1, 0, 0, 0], [1, 0, 0, 1], [1, 0, 1, 0]]
    graph = [[0, 1, 0, 0, 0, 0], [1, 0, 0, 0, 0, 0], [0, 0, 0, 1, 1, 0], [0, 0, 1, 0, 0, 0], [0, 0, 1, 0, 0, 1],
             [0, 0, 0, 0, 1, 0]]
    # print(graph[0])
    graph = torch.LongTensor(graph)
    # print(graph)
    return graph


def neighbor_embedding(current_embed, neighbor_graph):
    sum_embedding = torch.mm(neighbor_graph, current_embed)  # (k,n) * (n,d) = (k, d)
    sum_neighbor = torch.sum(neighbor_graph, dim=1)  # (k)
    k = sum_neighbor.shape[0]
    flag_tensor = torch.ones_like(sum_neighbor)
    torch.where(sum_neighbor == 0, flag_tensor, sum_neighbor)
    aver_embedding = sum_embedding / sum_neighbor.view(k, -1)  # (k,d) ./ (k,d)

    return aver_embedding


class IAGR(nn.Module):
    def __int__(self, conf):
        self.path_length = conf.path_length
        self.fusion_group = nn.Linear(conf.fuse_dim, conf.embed_dim)
        self.fusion_item = nn.Linear(conf.fuse_dim, conf.embed_dim)
        self.sig_item = nn.Sigmoid()
        self.sig_group = nn.Sigmoid()
        self.W_1 = nn.Linear(conf.embed_dim * 2, conf.embed_dim)
        self.W_2 = nn.Linear(conf.embed_dim * 2, conf.embed_dim)
        self.W_3 = nn.Linear(conf.embed_dim * 2, conf.embed_dim)
        self.relu_1 = nn.LeakyReLU(inplace=False)
        self.relu_2 = nn.LeakyReLU(inplace=False)
        self.relu_3 = nn.LeakyReLU(inplace=False)
        # self.o_1 = neighbor_embedding()

    def forward(self, group_embed_init, item_embed_init, group_feat, item_feat, neighbor_graphs):
        concatenate_group = torch.cat([group_embed_init, group_feat], dim=1)
        concatenate_item = torch.cat([item_embed_init, item_feat], dim=1)
        group_embedding = self.sig_group(self.fusion_group(concatenate_group))
        item_embedding = self.sig_item(self.fusion_item(concatenate_item))

        group_embedding_list = []
        group_embedding_first = neighbor_embedding(group_embedding, neighbor_graphs[0])  # (k,d)
        concatenate_group_embedding_first = torch.cat([group_embedding, group_embedding_first], dim=1)
        group_embedding_first = self.relu_1(self.W_1(concatenate_group_embedding_first))  # (k,d)
        group_embedding_list.append(group_embedding_first)
        if self.path_length > 1:
            group_embedding_second = neighbor_embedding(group_embedding_first, neighbor_graphs[0])  # (k,d)
            concatenate_group_embedding_second = torch.cat([group_embedding_first, group_embedding_second], dim=1)
            group_embedding_second = self.relu_2(self.W_2(concatenate_group_embedding_second))
            group_embedding_list.append(group_embedding_second)

        if self.path_length > 2:
            group_embedding_third = neighbor_embedding(group_embedding_second, neighbor_graphs[0])  # (k,d)
            concatenate_group_embedding_third = torch.cat([group_embedding_second, group_embedding_third], dim=1)
            group_embedding_third = self.relu_3(self.W_2(concatenate_group_embedding_third))
            group_embedding_list.append(group_embedding_third)


def gene_neighbor_seed(seeds, graph, contained_nodes):
    list = [0, 1, 3]
    tmp = graph[seeds]
    [w, h] = tmp.shape
    id_list = []
    print(w, h)
    tmp = tmp.data.numpy()
    print(tmp)
    for i in range(w):
        cur_tmp = tmp[i]
        contained_node = contained_nodes[i]
        print(cur_tmp)
        id_ = np.where(cur_tmp == 1)
        print(id_[0], type(id_))
        A = set(contained_node)
        B = set(id_[0])

        print('neighbor: ', B - A)
        print('new_contain:', B | A)


def gene_neighbor(graph, contained_nodes):
    tmp = graph

    [w, h] = tmp.shape
    id_list = []
    # print(w, h)
    tmp = tmp.data.numpy()
    # print(tmp)
    graph_new = []
    contained_nodes_new = []
    for i in range(w):
        cur_tmp = tmp[i]
        contained_node = contained_nodes[i]
        # print(cur_tmp)
        id_ = np.where(cur_tmp != 0)
        # print(id_[0], type(id_))
        A = set(contained_node)
        B = set(id_[0])
        ex_nodes = list(B & A)
        neighbor = list(B - A)

        graph[i][ex_nodes] = 0
        # graph_new.append(tmp_neigh)
        tmp_contained = list(B | A)
        contained_nodes_new.append(tmp_contained)
        # print('neighbor: ', neighbor)
        # print('new_contain:', tmp_contained)

    # graph_new = torch.stack(graph_new, dim=0)
    print(contained_nodes_new)
    print(graph)
    # print(graph_new)
    return graph, contained_nodes_new


def construct_graph_order():
    G = graph_construction()
    nodes = []
    for v in range(6):
        nodes.append([v])
    print('first:\n', G)
    G_ = G.clone()
    first_neighbor, nodes = gene_neighbor(G_, nodes)
    second_G = torch.mm(G, G)
    print('second:\n', second_G)
    second_neighbor, nodes = gene_neighbor(second_G, nodes)
    third_G = torch.mm(second_G, G)
    print('third:\n', third_G)
    third_neighbor, nodes = gene_neighbor(third_G, nodes)
    return first_neighbor, second_neighbor, third_neighbor


def parameter_revised(tensor_test):
    tensor_test[[0, 1, 2]] = 0
    return tensor_test


if __name__ == '__main__':
    net = IAGR()
    torch.manual_seed(1024)
    embedding_func = nn.Embedding(2, 5)
    embedding_func = nn.Embedding(2000, 64)

    batch = embedding_func(torch.LongTensor([0, 1]))
    print(batch)

    A = [[1, 2, 3], [0, 0, 0]]
    B = [[1, 1, 1], [0, 0, 0]]
    a_ = torch.Tensor(A)
    a_1 = torch.Tensor(B)
    b = torch.cat([a_, a_1], dim=1)
    print(b, '\n', b.shape)
    c = torch.sum(b, dim=1)
    flag_tensor = torch.ones_like(c)
    c = torch.where(c == 0, flag_tensor, c)

    print(c, '\n', c.shape[0])
    e = b.t() * c

    # e = torch.unsqueeze(c, dim=1)
    # e = F.pad(input=e, pad=(0, 5), mode='replicate')
    print(b)
    print(e.t())
    f = b / c.view(2, -1)
    print(f)

    # diag = torch.diag(second_G)
    # diag_mat = torch.diag_embed(diag)
    # # print(second_G - diag_mat)
    # third_G = torch.mm(second_G - diag_mat, G)
    # diag = torch.diag(third_G)
    # diag_mat = torch.diag_embed(diag)
    # print(third_G - diag_mat)
