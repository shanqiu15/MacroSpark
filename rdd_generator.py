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


class SparkContext():
    def __init__(self, worker_list):
        #setup cluster worker connections
        self.workers  = worker_list
        self.connections = []
        for index, worker in enumerate(worker_list):
            if self.index != index:
                c = zerorpc.Client(timeout=1)
                c.connect("tcp://" + worker)
                self.connections.append(c)

def visit_lineage(rdd):
    for i in rdd.get_lineage():
        op = next(i)
        options[op.__class__.__name__]()

# define the function blocks
def visitTextFile():
    print "visit TestFile.\n"

def visitMap():
    print "visit Map\n"

def visitFilter():
    print "visit Filter\n"

def visitFlatmap():
    print "visit FlatMap\n"

def visitReduceByKey():
    print "visit ReduceByKey\n"

def visitMapValue():
    print "visit MapValue\n"

def visitJoin():
    print "visit join\n"

# map the inputs to the function blocks
options = { "TextFile"    : visitTextFile,
            "Map"         : visitMap,
            "Filter"      : visitFilter,
            "FlatMap"     : visitFlatmap,
            "ReduceByKey" : visitReduceByKey,
            "MapValue"    : visitMapValue,
            "Join"        : visitJoin,
}







if __name__ == "__main__":
    # j = Join("old_1", "old_2");
    # for i in j.get():
    #     print i
    # print j.get()
    r = TextFile('myfile')
    m = Map(r, lambda s: s.split())
    f = Filter(m, lambda a: int(a[1]) > 2)
    mv = MapValue(f, lambda s:s)
    r = ReduceByKey(mv, lambda x, y: x + y)
    # print f.collect(), f.count()

    visit_lineage(r)
    # for i in r.get_lineage():
    #     op = next(i)
    #     print op.id
    #     print op.__class__.__name__
    #i = r.get_lineage.pop()
