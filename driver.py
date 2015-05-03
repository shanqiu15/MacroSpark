from rdd import *
from partition import *

class SparkContext():

    # def __init__(self, worker_list):
    #     #setup cluster worker connections
    #     self.workers  = worker_list
    #     self.connections = []
    #     for index, worker in enumerate(worker_list):
    #         if self.index != index:
    #             c = zerorpc.Client(timeout=1)
    #             c.connect("tcp://" + worker)
    #             self.connections.append(c)

    def __init__(self):

        #key: rdd_id, value: partition operation object
        self.operations = {}

        self.statges = []
        # map the inputs to the function blocks
        self.options = { "TextFile"    : self.visitTextFile,
                         "Map"         : self.visitMap,
                         "Filter"      : self.visitFilter,
                         "FlatMap"     : self.visitFlatmap,
                         "ReduceByKey" : self.visitReduceByKey,
                         "MapValue"    : self.visitMapValue,
                         "Join"        : self.visitJoin,
                        }


    def visit_lineage(self, rdd):
        for i in rdd.get_lineage():
            op = next(i)
            self.options[op.__class__.__name__](op)

    # define the function blocks
    def visitTextFile(self, textfile):
        print "visit TestFile.\n"
        print textfile.__class__.__name__
        print textfile.filePath
        print textfile.get_parent(), "\n"
        self.operations[textfile.id] = FilePartition(textfile.id, textfile.filePath)
        # for i in range(len(worker_list)):
        #     r[i] = FilePartition(textfile, textfile.id, i)


    def visitMap(self, mapper):
        print "visit Map"
        print mapper.get_parent(), "\n"
        parent = mapper.get_parent()
        self.operations[mapper.id] = MapPartition(mapper.id, self.operations[parent.id], mapper.func)

        # return MapPartition(rdd_id, parent, func)
        # for i in range(len(worker_list)):
        #     m[i] = 

    def visitFilter(self, filt):
        print "visit Filter"
        print filt.get_parent(), "\n"
        parent = filt.get_parent()
        self.operations[filt.id] = FilterPartition(filt.id, self.operations[parent.id], filt.func)

    def visitFlatmap(self, flatMap):
        print "visit FlatMap"
        print flatMap.get_parent(), "\n"
        parent = flatMap.get_parent()
        self.operations[flatMap.id] = FlatMapPartition(flatMap.id, self.operations[parent.id], flatMap.func)

    def visitReduceByKey(self, reduceByKey):
        print "visit ReduceByKey"
        print reduceByKey.get_parent(), "\n"
        parent = reduceByKey.get_parent()
        self.operations[reduceByKey.id] = ReduceByKeyPartition(reduceByKey.id, self.operations[parent.id], reduceByKey.func)

    def visitMapValue(self, mapValue):
        print "visit MapValue"
        print mapValue.get_parent(), "\n"
        parent = mapValue.get_parent()
        self.operations[mapValue.id] = MapValuePartition(mapValue.id, self.operations[parent.id], mapValue.func)

    def visitJoin(self, join):
        print "visit join"
        print join.get_parent(),"\n"
        parent = mapValue.get_parent()
        self.operations[join.id] = JoinPartition(join.id, self.operations[parent[0].id],self.operations[parent[1].id])

    def collect(self, rdd):
        pass

    def count(self, rdd):
        pass

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

    
    sc = SparkContext()
    sc.visit_lineage(r)
    # for i in r.get_lineage():
    #     op = next(i)
    #     print op.id
    #     print op.__class__.__name__
    #i = r.get_lineage.pop()
