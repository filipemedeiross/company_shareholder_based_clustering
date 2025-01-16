import numpy as np
import matplotlib.pyplot as plt
import scipy.cluster.hierarchy as scipy

from .tools import normalize


def dendrogram(L, n, l, f_labels=None):
    label = lambda x: f_labels[int(x)]  \
            if f_labels                 \
            else lambda x: f'X{int(x)}'

    figs = []
    for Li in L:
        indices = Li[:, :2].flatten()
        labels  = [label(idx)
                   for idx in np.sort(indices[np.where(indices < n)])]

        x_min = Li[:, 2].min() - 1
        x_max = Li[:, 2].max() + 1

        fig, _ = plt.subplots()

        scipy.dendrogram(normalize(Li),
                         labels=labels,
                         orientation='right')
        plt.xlim((x_min, x_max))
        plt.xticks(plt.xticks()[0], l - plt.xticks()[0])

        figs.append(fig)

    return figs
