import numpy as np
import torch
import torch.nn as nn
import dgl
from dgl.data import CoraGraphDataset


def gcn_msg(edge):
    msg = edge.src['h'] * edge.src['norm']
    return {'m': msg}


def gcn_reduce(node):
    accum = torch.sum(node.mailbox['m'], 1) * node.data['norm']
    return {'h': accum}


class NodeApplyModule(nn.Module):
    def __init__(self, in_feats, out_feats):
        """
        in_feats: 特征输入的维度
        out_feats：特征输出的维度
        """
        super(NodeApplyModule, self).__init__()
        self.linear = nn.Linear(in_features=in_feats, out_features=out_feats)
        self.activation = nn.Tanh()

    def forward(self, node):
        """
        node：图的节点
        返回字典形式
        """
        feats = node.data['h']
        h = self.linear(feats)
        h = self.activation(h)
        return {'h': h}


class GCNLayer(nn.Module):
    def __init__(self, in_feats, out_feats):
        """
        in_feats: 特征输入的维度
        out_feats：特征输出的维度
        """
        super(GCNLayer, self).__init__()
        self.apply_node = NodeApplyModule(in_feats, out_feats)  # 引入更新函数

    def forward(self, g, features):
        """
        g: 通过DGL定义的graph
        features：图的节点特征
        """
        g.ndata['h'] = features  # 将节点特征，传入图的属性
        g.update_all(gcn_msg, gcn_reduce)  # 消息传递和聚合
        g.apply_nodes(func=self.apply_node)  # 更新
        h = g.ndata.pop('h')  # 提取计算后的节点特征，并删除Graph中的对应属性
        return h


class Net(nn.Module):
    def __init__(self, in_feats, out_feats):
        super(Net, self).__init__()
        self.gcn1 = GCNLayer(in_feats=in_feats, out_feats=16)
        self.gcn2 = GCNLayer(in_feats=16, out_feats=out_feats)

    def forward(self, g, features):
        h = features
        h_1 = self.gcn1(g, h)
        h_2 = self.gcn2(g, h_1)
        return h_1, h_2


# 实例化模型
if __name__ == '__main__':

    data = CoraGraphDataset()  # 采用Cora数据进行实验
    g = data[0]  # 只用一个graph
    features = g.ndata['feat']
    labels = g.ndata['label']
    train_mask = g.ndata['train_mask']
    val_mask = g.ndata['val_mask']
    test_mask = g.ndata['test_mask']
    in_feats = features.shape[1]
    n_classes = data.num_labels
    # n_edges = data.graph.number_of_edges()
    print(in_feats)
    g = dgl.remove_self_loop(g)
    g = dgl.add_self_loop(g)
    n_edges = g.number_of_edges()

    # normalization
    degs = g.in_degrees().float()  # 获取节点的度
    norm = torch.pow(degs, -0.5)
    norm[torch.isinf(norm)] = 0
    g.ndata['norm'] = norm.unsqueeze(1)
    model = Net(in_feats=1433, out_feats=7)

    # loss
    loss_fcn = torch.nn.CrossEntropyLoss()

    # use optimizer
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    # 训练模型
    for epoch in range(20):
        model.train()
        # forward
        H1, logits = model(g, features)
        loss = loss_fcn(logits[train_mask], labels[train_mask])

        # backward
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        print('Epoch: {:5d} | Loss: {:.3f}'.format(epoch, loss.item()))
    print(features[1])
    print(H1[1])
