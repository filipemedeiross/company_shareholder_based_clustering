class Cluster:
    def __init__(self, idx, v, l):
        self.id = idx  # cluster index
        self.v  =   v  # vertices that integrate the cluster
        self.l  =   l  # common resources in the adjacency


class FindUnion:
    def __init__(self, n):
        self.v = list(range(2 * n - 1))

    def __getitem__(self, u):
        while u != self.v[u]:
            self.v[u] = self.v[self.v[u]]
            u = self.v[u]

        return u

    def union(self, u, v):
        root_u = self.__getitem__(u)
        root_v = self.__getitem__(v)

        self.v[root_v] = root_u


class DisjointSet:
    def __init__(self, n):
        self.v = list(range(n))

    def __getitem__(self, u):
        while u != self.v[u]:
            self.v[u] = self.v[self.v[u]]
            u = self.v[u]

        return u

    def union(self, u, v):
        root_u = self.__getitem__(u)
        root_v = self.__getitem__(v)

        self.v[root_v] = root_u
