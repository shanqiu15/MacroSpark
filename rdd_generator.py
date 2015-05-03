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
        self.type = "TextFile"

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
        print "This is the get in TextFile"
        yield self

class Map(RDD):

    def __init__(self, parent, func):
        super(Map, self).__init__()
        self.parent = parent
        self.func = func
        self.type = "Map"
        #Store all the operations until now (a generator list)
        self.lineage = []
        self.set_lineage()


    def need_repartition(self):
        return False

    def set_lineage(self):
        parent  = next(self.parent.get())
        self.lineage = parent.get_lineage()
        self.lineage.append(self.get())

    #each partition will call yield and seperate the lineage to different stages
    def get_lineage(self):
        return self.lineage

    def get(self):
        yield self

class Filter(RDD):
    
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func
        self.type = "Filter"

    def need_repartition(self):
        return False


    # def get(self):
    #     print "This is the caculation in filter"
    #     parent_generator = self.parent.get()
    #     parent = next(parent_generator)
    #     self.lineage = rdd.lineage.append(self.parent.get())
    #     yield self

class FlatMap(RDD):
    def __init__(self, parent):
        self.parent = parent
        self.func = func
        self.type = "FlatMap"

    def need_repartition(self):
        return False

    def get(self):   
        parent_generator = self.parent.get()
        next(parent_generator)
        print "This is the caculation in flatMap"
        yield self

class ReduceByKey(RDD):
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func
        self.type = "ReduceByKey"

    def need_repartition(self):
        return True

    def get(self):
        print "This is a ReduceByKey"
        parent_generator = self.parent.get()
        next(parent_generator)
        yield self

class MapValue(RDD):
    def __init__(self, parent, func):
        self.parent = parent
        self.func =func
        self.type = "MapValue"

    def need_repartition(self):
        return False

    def get(self):
        
        parent_generator = self.parent.get()
        next(parent_generator)
        print "This is a MapValue"
        yield self

class Join(RDD):
    def __init__(self, parent, rhd_rdd):
        self.parent = parent
        self.rhs_rdd = rhs_rdd
        self.type = "Join"

    def need_repartition(self):
        return True

    def get(self):
        print "This is a Join"
        parent_generator = self.parent.get()
        next(parent_generator)
        yield self




if __name__ == "__main__":
    # j = Join("old_1", "old_2");
    # for i in j.get():
    #     print i
    # print j.get()
    r = TextFile('myfile')
    m = Map(r, lambda s: s.split())
    m = Map(m, lambda s: s)
    m = Map(m, lambda s: s)
    # m.get_lineage()
    for i in m.get_lineage():
        print next(i).id
    # f = Filter(m, lambda a: int(a[1]) > 2)
    # print f.collect(), f.count()

    