RDD_ID = 0

class RDD(object):

    def __init__(self):

        #Set a unique id for the RDD
        #ID will plus 2 here, so when we need a repartition between two RDDs
        #The id for the repartition result could be (RDD_ID_1+RDD_ID_2)/2
        global RDD_ID
        RDD_ID = RDD_ID + 2
        self.id = RDD_ID
        self.collect = False
        self.count = False

    def get_parent(self):
        return self.parent

    def set_lineage(self):
        self.lineage = []
        parent = self.get_parent()
        self.lineage = parent.get_lineage()
        self.lineage.append(self)

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        self.set_lineage()
        return self.lineage

    def rdd_collect(self):
        self.collect = True

    def rdd_count(self):
        self.count = True


class TextFile(RDD):

    def __init__(self, filePath):
        super(TextFile, self).__init__()
        self.filePath = filePath
        self.parent = None

        #Store all the operations until now (a generator list)
        self.set_lineage()

    def need_repartition(self):
        return False

    def set_lineage(self):
        self.lineage = []
        self.lineage.append(self)


class Map(RDD):

    def __init__(self, parent, func):
        super(Map, self).__init__()
        self.parent = parent
        self.func = func

        #Store all the operations until now (a generator list)
        self.set_lineage()

    def need_repartition(self):
        return False


class Filter(RDD):
    
    def __init__(self, parent, func):
        super(Filter, self).__init__()
        self.parent = parent
        self.func = func

        #Store all the operations until now (a generator list)
        self.set_lineage()

    def need_repartition(self):
        return False


class FlatMap(RDD):
    def __init__(self, parent, func):
        super(FlatMap, self).__init__()
        self.parent = parent
        self.func = func

        #Store all the operations until now (a generator list)
        self.set_lineage()

    def need_repartition(self):
        return False


class ReduceByKey(RDD):
    def __init__(self, parent, func):
        self.parent = RePartition(parent)
        super(ReduceByKey, self).__init__()
        # self.parent = parent
        self.func = func

        #Store all the operations until now (a generator list)
        self.set_lineage()

    def need_repartition(self):
        return True


class GroupByKey(RDD):
    def __init__(self, parent):
        self.parent = RePartition(parent)
        super(GroupByKey, self).__init__()

        #Store all the operations until now (a generator list)
        self.set_lineage()

    def need_repartition(self):
        return True    


class MapValue(RDD):
    def __init__(self, parent, func):
        super(MapValue, self).__init__()
        self.parent = parent
        self.func =func

        #Store all the operations until now (a generator list)
        self.set_lineage()

    def need_repartition(self):
        return False


class Join(RDD):
    def __init__(self, parent_0, parent_1):
        self.parent = []
        self.parent.append(RePartition(parent_0))
        self.parent.append(RePartition(parent_1))
        super(Join, self).__init__()
        #Store all the operations until now (a generator list)
        self.set_lineage()

    def need_repartition(self):
        return True

    def set_lineage(self):
        self.lineage = []
        parent_0 = self.parent[0]
        parent_1 = self.parent[1]
        self.lineage = parent_0.get_lineage() + parent_1.get_lineage()
        self.lineage.append(self)


class RePartition(RDD):
    def __init__(self, parent, func = None):
        super(RePartition, self).__init__()

        #Split the lineage stage befor each repartition
        self.parent = parent
        self.func = func

    def need_repartition(self):
        return False



if __name__ == "__main__":
    r = TextFile('myfile')
    m = Map(r, lambda s: s.split())
    f = Filter(m, lambda a: int(a[1]) > 2)
    mv = MapValue(f, lambda s:s)
    z = ReduceByKey(mv, lambda x, y: x + y)

    r2 = TextFile('myfile')
    m2 = Map(r, lambda s: s.split())
    j = Join(f, m2)

    for op in j.get_lineage():
        print op.id
        print op.__class__.__name__


