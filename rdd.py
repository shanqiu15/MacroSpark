RDD_ID = 0

class RDD(object):

    def __init__(self):

        #Set a unique id for the RDD
        global RDD_ID
        RDD_ID = RDD_ID + 1
        self.id = RDD_ID


    # def collect(self):
    #     elements = []
    #     for element in self.get():
    #         elements.append(element)
    #     return elements

    # def count(self):
    #     return len(self.collect())

class TextFile(RDD):

    def __init__(self, filename):
        super(TextFile, self).__init__()
        self.filename = filename

        #Store all the operations until now (a generator list)
        self.lineage = []
        self.set_lineage()

    def need_repartition(self):
        return False

    def set_lineage(self):
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        return self.lineage

    def get(self):
        yield self

class Map(RDD):

    def __init__(self, parent, func):
        super(Map, self).__init__()
        self.parent = parent
        self.func = func

        #Store all the operations until now (a generator list)
        self.lineage = []
        self.set_lineage()


    def need_repartition(self):
        return False

    def set_lineage(self):
        parent = next(self.parent.get())
        self.lineage = parent.get_lineage()
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        return self.lineage

    def get(self):
        yield self

class Filter(RDD):
    
    def __init__(self, parent, func):
        super(Filter, self).__init__()
        self.parent = parent
        self.func = func

        #Store all the operations until now (a generator list)
        self.lineage = []
        self.set_lineage()

    def need_repartition(self):
        return False

    def set_lineage(self):
        parent = next(self.parent.get())
        self.lineage = parent.get_lineage()
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        return self.lineage

    def get(self):
        yield self

class FlatMap(RDD):
    def __init__(self, parent):
        super(FlatMap, self).__init__()
        self.parent = parent
        self.func = func

        #Store all the operations until now (a generator list)
        self.lineage = []
        self.set_lineage()

    def need_repartition(self):
        return False

    def set_lineage(self):
        parent = next(self.parent.get())
        self.lineage = parent.get_lineage()
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        return self.lineage

    def get(self):
        yield self

class ReduceByKey(RDD):
    def __init__(self, parent, func):
        super(ReduceByKey, self).__init__()
        self.parent = parent
        self.func = func

        #Store all the operations until now (a generator list)
        self.lineage = []
        self.set_lineage()

    def need_repartition(self):
        return False

    def set_lineage(self):
        parent = next(self.parent.get())
        self.lineage = parent.get_lineage()
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        return self.lineage

    def get(self):
        yield self

class MapValue(RDD):
    def __init__(self, parent, func):
        super(MapValue, self).__init__()
        self.parent = parent
        self.func =func

        #Store all the operations until now (a generator list)
        self.lineage = []
        self.set_lineage()

    def need_repartition(self):
        return False

    def set_lineage(self):
        parent = next(self.parent.get())
        self.lineage = parent.get_lineage()
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        return self.lineage

    def get(self):
        yield self

class Join(RDD):
    def __init__(self, parent, rhd_rdd):
        super(Join, self).__init__()
        self.parent = parent
        self.rhs_rdd = rhs_rdd

        #Store all the operations until now (a generator list)
        self.lineage = []
        self.set_lineage()

    def need_repartition(self):
        return False

    def set_lineage(self):
        parent = next(self.parent.get())
        self.lineage = parent.get_lineage()
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        return self.lineage

    def get(self):
        yield self
