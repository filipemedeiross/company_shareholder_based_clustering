import numpy as np
from scipy.sparse import csr_matrix


def distance(L, Ci, Cj):
    prod = Ci.l @ Cj.l.T
    sim  = prod.nnz and prod.data[0]

    return L - sim

def similarity(L, dist):
    return L - dist

def hc_steps(idx, C, l):
    v, Cv = idx, C[idx]

    min_u = None
    min_dist = l

    steps = []
    for u, Cu in C.items():
        if u == v:
            continue

        d = distance(l, Cu, Cv)

        if d == 0:
            steps.append(u)
        elif d < min_dist:
            min_u    = u
            min_dist = d

    if min_u:
        steps.append(min_u)

    return steps

def list2matrix(adjlist, n, l):
    data = []
    rows = []
    cols = []

    for u, adj in enumerate(adjlist):
        for v in adj:
            data.append(1)
            rows.append(u)
            cols.append(v)

    return csr_matrix((data, (rows, cols)), shape=(n, l), dtype='uint32')

def normalize(L):
    argsort = np.argsort(L[:, :2].flatten())

    ind = np.arange(len(argsort))
    ind = np.array(sorted(ind, key=lambda i: argsort[i]))

    L_ = L.copy()
    L_[:, :2] = ind.reshape(L[:, :2].shape)

    return L_
