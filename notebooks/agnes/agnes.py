import numpy as np
from .cluster import Cluster
from .tools   import distance,   \
                     hc_steps,   \
                     similarity, \
                     list2matrix


def agnes(idx, adjlist, n, l, thresh=1):
    adjmatrix = list2matrix(adjlist, n, l)

    L = []
    C = {v : Cluster(v, [v], adj)
         for v, adj in enumerate(adjmatrix)}

    m = idx
    v =   n

    while True:
        steps = hc_steps(m, C, C[m].l.nnz)

        if not steps:
            return np.array(L, dtype='float64')

        for k in steps:
            C_k = C[k]
            C_m = C[m]

            d = distance(l, C_k, C_m)
            if thresh > similarity(l, d):
                return np.array(L, dtype='float64')

            C[v] = C_v = Cluster(v,                     \
                                 C_k.v + C_m.v,         \
                                 C_k.l.multiply(C_m.l))

            L.append([C_k.id, C_m.id, d, len(C_v.v)])

            del C[k], C[m]

            m = v
            v = v + 1
