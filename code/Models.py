# -- coding: utf-8 --
import numpy as np
import torch
import torch.nn as nn
import random
from utilize import generate_attribute_feats, generate_matrics

random.seed(2)
np.random.seed(2)
torch.manual_seed(2)
torch.cuda.manual_seed_all(2)


class configue:
    def __init__(self):
        self.latent_dim = ''
        self.num_groups = ''
        self.num_items = ''
        self.gi_m = ''
        self.gg_m = ''
        self.g_feat = ''
        self.i_feat = ''
        self.deg = ''
        self.L = ''

# def init_model(pre_def):
#     conf = configue()
#     conf.latent_dim = 64
#     conf.num_groups = 110
#     conf.num_items = 100
#     # generate_attribute_feats(1)
#     gg_m, gi_m = generate_matrics(conf.num_groups, conf.num_items, pre_def)
#     g_feat, i_feat = generate_attribute_feats(1)
#     conf.gi_m = gi_m
#     conf.gg_m = gg_m
#     conf.g_feat = g_feat
#     conf.i_feat = i_feat
#     model = IAGR(conf)
#     model.initializeNodes()
#     return model


class LstmFcAutoEncoder(nn.Module):
    def __init__(self, input_layer=300, hidden_layer=100, batch_size=20):
        super(LstmFcAutoEncoder, self).__init__()

        self.input_layer = input_layer
        self.hidden_layer = hidden_layer
        self.batch_size = batch_size

        self.encoder_gru = nn.GRU(self.input_layer, self.hidden_layer, batch_first=True)
        self.encoder_fc = nn.Linear(self.hidden_layer, self.hidden_layer)
        self.decoder_gru = nn.GRU(self.hidden_layer, self.input_layer, batch_first=True)
        self.decoder_fc = nn.Linear(self.hidden_layer, self.hidden_layer)
        self.relu = nn.ReLU()

    def forward(self, input_x):
        input_x = input_x.view(len(input_x), 1, -1)
        # encoder
        hidden, (n, c) = self.encoder_gru(input_x,
                                          # shape: (n_layers, batch, hidden_size)
                                          (torch.zeros(1, self.batch_size, self.hidden_layer),
                                           torch.zeros(1, self.batch_size, self.hidden_layer)))
        hidden_fc = self.encoder_fc(hidden)
        hidden_out = self.relu(hidden_fc)
        # decoder
        decoder_fc = self.relu(self.decoder_fc(hidden_out))
        decoder_lstm, (n, c) = self.decoder_gru(decoder_fc,
                                                (torch.zeros(1, 20, self.input_layer),
                                                 torch.zeros(1, 20, self.input_layer)))
        return decoder_lstm.squeeze()


def init_model_large(pre_def):
    conf = configue()
    conf.latent_dim = 64
    conf.num_groups = 2000
    conf.num_items = 1500
    gg_ratio = 0.025
    gi_ratio = 0.01
    names = ['R_gg_large.pt', 'R_gi_large.pt']
    names_gi = ['g_feat_large.pt', 'i_feat_large.pt']
    gg_m, gi_m = generate_matrics(conf.num_groups, conf.num_items, gg_ratio, gi_ratio, names, pre_def)
    g_feat, i_feat = generate_attribute_feats(conf.num_groups, conf.num_items, names_gi, pre_def)
    conf.gi_m = gi_m
    conf.gg_m = gg_m
    deg = torch.sparse.sum(gg_m, dim=1)
    conf.deg = (deg.to_dense() - 1)
    conf.g_feat = g_feat
    conf.i_feat = i_feat
    model = IGR(conf)
    model.initializeNodes()
    return model


def init_model_yelp(pre_def, L):
    conf = configue()
    conf.latent_dim = 64
    conf.num_groups = 4000
    conf.num_items = 3000
    gg_ratio = 0.015
    gi_ratio = 0.0015
    names_gg = ['R_gg_yelp.pt', 'R_gi_yelp.pt']
    names_gi = ['g_feat_yelp.pt', 'i_feat_yelp.pt']
    gg_m, gi_m = generate_matrics(conf.num_groups, conf.num_items, gg_ratio, gi_ratio, names_gg, pre_def)
    g_feat, i_feat = generate_attribute_feats(conf.num_groups, conf.num_items, names_gi, pre_def)
    conf.gi_m = gi_m
    conf.gg_m = gg_m
    deg = torch.sparse.sum(gg_m, dim=1)
    conf.deg = (deg.to_dense() - 1)
    conf.g_feat = g_feat
    conf.i_feat = i_feat
    conf.L = L
    model = IGR(conf)
    model.initializeNodes()
    return model


def init_model_movie(pred, L):
    conf = configue()
    conf.latent_dim = 64
    conf.num_groups = 1350
    conf.num_items = 3883
    gg_ratio = 0.
    gi_ratio = 0.
    names_gg = ['R_gg_movie.pt', 'R_gi_movie.pt']
    names_gi = ['g_feat_movie.pt', 'i_feat_movie.pt']
    gg_m, gi_m = generate_matrics(conf.num_groups, conf.num_items, gg_ratio, gi_ratio, names_gg, 1)
    g_feat, i_feat = generate_attribute_feats(conf.num_groups, conf.num_items, names_gi, 1)
    conf.gi_m = gi_m
    conf.gg_m = gg_m
    deg = torch.sparse.sum(gg_m, dim=1)
    conf.deg = (deg.to_dense() - 1)
    conf.g_feat = g_feat
    conf.i_feat = i_feat
    conf.L = L
    model = IGR(conf)
    model.initializeNodes()
    return model


def init_model_mfw(pred, L):
    conf = configue()
    conf.latent_dim = 64
    conf.num_groups = 995
    conf.num_items = 1513
    gg_ratio = 0.
    gi_ratio = 0.
    names_gg = ['R_gg_mfw.pt', 'R_gi_mfw.pt']
    names_gi = ['g_feat_mfw.pt', 'i_feat_mfw.pt']
    gg_m, gi_m = generate_matrics(conf.num_groups, conf.num_items, gg_ratio, gi_ratio, names_gg, 1)
    g_feat, i_feat = generate_attribute_feats(conf.num_groups, conf.num_items, names_gi, 1)
    conf.gi_m = gi_m
    conf.gg_m = gg_m
    deg = torch.sparse.sum(gg_m, dim=1)
    conf.deg = (deg.to_dense() - 1)
    conf.g_feat = g_feat
    conf.i_feat = i_feat
    conf.L = L
    model = IGR(conf)
    model.initializeNodes()
    return model


def init_model_YELP(pred, L):
    conf = configue()
    conf.latent_dim = 64
    conf.num_groups = 24103
    conf.num_items = 22611
    gg_ratio = 0.0007
    gi_ratio = 0.0002
    names_gg = ['R_gg_yelp.pt', 'R_gi_yelp_.pt']  # R_gi_movie.pt
    names_gi = ['g_feat_yelp_.pt', 'i_feat_yelp_.pt']
    gg_m, gi_m = generate_matrics(conf.num_groups, conf.num_items, gg_ratio, gi_ratio, names_gg, 1)
    g_feat, i_feat = generate_attribute_feats(conf.num_groups, conf.num_items, names_gi, 1)
    conf.gi_m = gi_m
    conf.gg_m = gg_m
    deg = torch.sparse.sum(gg_m, dim=1)
    conf.deg = (deg.to_dense() - 1)
    conf.g_feat = g_feat
    conf.i_feat = i_feat
    conf.L = L
    model = IGR(conf)
    model.initializeNodes()
    return model


class IGR(nn.Module):
    def __init__(self, conf):
        super().__init__()
        # self.conf = conf
        self.latent_dim = conf.latent_dim
        self.num_items = conf.num_items
        self.num_groups = conf.num_groups
        self.consumed_items_sparse_matrix = conf.gi_m
        self.social_neighbors_sparse_matrix = conf.gg_m
        self.degree = conf.deg
        self.group_feat_vector_matrix = conf.g_feat
        self.item_feat_vector_matrix = conf.i_feat
        self.W = nn.Linear(self.latent_dim, self.latent_dim)
        self.relu = nn.ReLU()
        self.act = nn.Sigmoid()
        self.L = conf.L
        print(self.social_neighbors_sparse_matrix.dtype)
        print(self.consumed_items_sparse_matrix.dtype)
        print(self.group_feat_vector_matrix.dtype)
        print(self.item_feat_vector_matrix.dtype)

    def generateGroupEmebddingFromConsumedItems(self, current_item_embedding):
        group_embedding_from_consumed_items = torch.sparse.mm(self.consumed_items_sparse_matrix, current_item_embedding)
        return (group_embedding_from_consumed_items)

    def generateGroupEmbeddingFromSocialNeighbors(self, current_group_embedding):
        group_embedding_from_social_neighbors = torch.sparse.mm(self.social_neighbors_sparse_matrix,
                                                                current_group_embedding)
        return self.relu(self.W(group_embedding_from_social_neighbors))

    def startConstructGraph(self):
        self.initializeNodes()
        self.constructTrainGraph()

    def initializeNodes(self):
        # self.num_groups = self.num_groups
        self.embedding_group = torch.nn.Embedding(num_embeddings=self.num_groups, embedding_dim=self.latent_dim)
        self.embedding_item = torch.nn.Embedding(num_embeddings=self.num_items, embedding_dim=self.latent_dim)
        nn.init.normal_(self.embedding_group.weight, std=0.1)
        nn.init.normal_(self.embedding_item.weight, std=0.1)
        # print(self.embedding_group.weight)
        self.reduce_dimension_layer = nn.Linear(self.latent_dim, self.latent_dim)
        self.item_fusion_layer = nn.Linear(self.latent_dim * 2, self.latent_dim)
        self.group_fusion_layer = nn.Linear(self.latent_dim * 2, self.latent_dim)

    def constructTrainGraph(self):
        norm = nn.InstanceNorm1d(0)
        first_group_feat_vector_matrix = norm(self.group_feat_vector_matrix)
        first_item_feat_vector_matrix = norm(self.item_feat_vector_matrix)
        self.group_reduce_dim_vector_matrix = self.reduce_dimension_layer(first_group_feat_vector_matrix)
        self.item_reduce_dim_vector_matrix = self.reduce_dimension_layer(first_item_feat_vector_matrix)
        second_group_feat_vector_matrix = norm(self.group_reduce_dim_vector_matrix)
        second_item_feat_vector_matrix = norm(self.item_reduce_dim_vector_matrix)
        # compute item embedding
        # self.fusion_item_embedding = self.item_fusion_layer(\
        #   tf.concat([self.item_embedding, second_item_feat_vector_matrix], 1))
        item_embed = self.embedding_item.weight
        if 1:
            item_cat_embedding = torch.concat((item_embed, second_item_feat_vector_matrix), dim=1)
            self.final_item_embedding = self.fusion_item_embedding = self.act(
                self.item_fusion_layer(item_cat_embedding))
        else:
            self.final_item_embedding = self.fusion_item_embedding = self.item_embedding + second_item_feat_vector_matrix

        # self.final_item_embedding = self.fusion_item_embedding = second_item_feat_vector_matrix

        # compute group embedding
        group_embedding_from_consumed_items = self.act(
            self.generateGroupEmebddingFromConsumedItems(self.final_item_embedding))

        group_embed = self.embedding_group.weight
        if 1:
            group_cat_embedding = torch.concat((group_embed, second_group_feat_vector_matrix), dim=1)
            self.fusion_group_embedding = self.act(self.group_fusion_layer(group_cat_embedding))
        else:
            self.fusion_group_embedding = group_embed + second_group_feat_vector_matrix

        if self.L == 0:
            self.final_group_embedding = self.act(
                (group_embedding_from_consumed_items + self.fusion_group_embedding) / 2
            )
        else:
            first_gcn_group_embedding = self.act(
                self.generateGroupEmbeddingFromSocialNeighbors(self.fusion_group_embedding))
            TMP_embedding = group_embedding_from_consumed_items + first_gcn_group_embedding
            if self.L > 1:
                second_gcn_group_embedding = self.act(
                    self.generateGroupEmbeddingFromSocialNeighbors(first_gcn_group_embedding))
                TMP_embedding += second_gcn_group_embedding
            if self.L > 2:
                third_gcn_group_embedding = self.act(
                    self.generateGroupEmbeddingFromSocialNeighbors(second_gcn_group_embedding))
                TMP_embedding += third_gcn_group_embedding
            if self.L > 3:
                forth_gcn_group_embedding = self.act(
                    self.generateGroupEmbeddingFromSocialNeighbors(third_gcn_group_embedding))
                TMP_embedding += forth_gcn_group_embedding
            self.final_group_embedding = self.act(TMP_embedding/(self.L))
        # self.final_group_embedding = second_gcn_group_embedding + group_embedding_from_consumed_items
        # FOLLOWING OPERATION IS USED TO TACKLE THE GRAPH OVER-SMOOTHING ISSUE
        # self.final_group_embedding = self.act(
        #     group_embedding_from_consumed_items
        #     + self.fusion_group_embedding
        #     + first_gcn_group_embedding
        #     + second_gcn_group_embedding
        #     + third_gcn_group_embedding
        #     + forth_gcn_group_embedding
        # )

        latest_group_latent = self.final_group_embedding
        latest_item_latent = self.final_item_embedding
        return latest_group_latent, latest_item_latent


    def generate_labels(self, group_ids, item_ids):
        label = []
        for group_id, item_id in zip(group_ids, item_ids):
            if self.consumed_items_sparse_matrix[group_id][item_id]:
                label.append(1)
            else:
                label.append(0)
        return np.array(label)

    def forward(self, group_ids, item_ids):
        # compute embedding
        all_groups, all_items = self.constructTrainGraph()
        # print('forward')
        # all_users, all_items = self.computer()
        all_neighbors = torch.spmm(self.social_neighbors_sparse_matrix, all_groups)
        groups_emb = all_groups[group_ids]
        items_emb = all_items[item_ids]
        groups_neighbors_emb = all_neighbors[group_ids]

        inner_pro = torch.mul(groups_emb, items_emb)
        similarity = torch.mul(groups_neighbors_emb, groups_emb)
        willingness = torch.mul(groups_neighbors_emb, items_emb)
        gamma_2 = torch.sum(similarity, dim=1) + torch.sum(willingness, dim=1)
        gamma = torch.sum(inner_pro, dim=1)
        return gamma, gamma_2
