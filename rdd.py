RDD_ID = 0

class RDD(object):

    def __init__(self):

        #Set a unique id for the RDD
        global RDD_ID
        RDD_ID = RDD_ID + 1
        self.id = RDD_ID

    def get_parent(self):
        return self.parent

    # def collect(self):
    #     elements = []
    #     for element in self.get():
    #         elements.append(element)
    #     return elements

    # def count(self):
    #     return len(self.collect())

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
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        self.set_lineage()
        return self.lineage

    def get(self):
        yield self

class Map(RDD):

    def __init__(self, parent, func):
        super(Map, self).__init__()
        self.parent = parent
        self.func = func

        #Store all the operations until now (a generator list)
        self.set_lineage()


    def need_repartition(self):
        return False

    def set_lineage(self):
        self.lineage = []
        parent = next(self.parent.get())
        self.lineage = parent.get_lineage()
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        self.set_lineage()
        return self.lineage

    def get(self):
        yield self

class Filter(RDD):
    
    def __init__(self, parent, func):
        super(Filter, self).__init__()
        self.parent = parent
        self.func = func

        #Store all the operations until now (a generator list)
        self.set_lineage()

    def need_repartition(self):
        return False

    def set_lineage(self):
        self.lineage = []
        parent = next(self.parent.get())
        self.lineage = parent.get_lineage()
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        self.set_lineage()
        return self.lineage

    def get(self):
        yield self

class FlatMap(RDD):
    def __init__(self, parent, func):
        super(FlatMap, self).__init__()
        self.parent = parent
        self.func = func

        #Store all the operations until now (a generator list)
        self.set_lineage()

    def need_repartition(self):
        return False

    def set_lineage(self):
        self.lineage = []
        parent = next(self.parent.get())
        self.lineage = parent.get_lineage()
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        self.set_lineage()
        return self.lineage

    def get(self):
        yield self

class ReduceByKey(RDD):
    def __init__(self, parent, func):
        super(ReduceByKey, self).__init__()
        self.parent = parent
        self.func = func

        #Store all the operations until now (a generator list)
        self.set_lineage()

    def need_repartition(self):
        return True

    def set_lineage(self):
        self.lineage = []
        parent = next(self.parent.get())
        self.lineage = parent.get_lineage()
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        self.set_lineage()
        return self.lineage

    def get(self):
        yield self

class MapValue(RDD):
    def __init__(self, parent, func):
        super(MapValue, self).__init__()
        self.parent = parent
        self.func =func

        #Store all the operations until now (a generator list)
        self.set_lineage()

    def need_repartition(self):
        return False

    def set_lineage(self):
        self.lineage = []
        parent = next(self.parent.get())
        self.lineage = parent.get_lineage()
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        self.set_lineage()
        return self.lineage

    def get(self):
        yield self

class Join(RDD):
    def __init__(self, parent_0, parent_1):
        super(Join, self).__init__()
        self.parent = [parent_0, parent_1]

        #Store all the operations until now (a generator list)
        self.set_lineage()

    def need_repartition(self):
        return True

    def set_lineage(self):
        self.lineage = []
        parent_0 = next(self.parent[0].get())
        parent_1 = next(self.parent[1].get())
        self.lineage = parent_0.get_lineage() + parent_1.get_lineage()
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        self.set_lineage()
        return self.lineage

    def get(self):
        yield self

if __name__ == "__main__":
    r = TextFile('myfile')
    m = Map(r, lambda s: s.split())
    f = Filter(m, lambda a: int(a[1]) > 2)
    mv = MapValue(f, lambda s:s)
    z = ReduceByKey(mv, lambda x, y: x + y)

    r2 = TextFile('myfile')
    m2 = Map(r, lambda s: s.split())
    j = Join(f, m2)

    for i in j.get_lineage():
        op = next(i)
        print op.id
        print op.__class__.__name__
    # i = r.get_lineage().pop()
    # print i

