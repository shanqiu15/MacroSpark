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
        print textfile.filePath, "\n"
        print textfile.get_parent()
        # for i in range(len(worker_list)):
        #     r[i] = FilePartition(textfile, textfile.id, i)


    def visitMap(self, mapper):
        print "visit Map\n"
        print mpper.get_parent()
        # for i in range(len(worker_list)):
        #     m[i] = 

    def visitFilter(self, filt):
        print "visit Filter\n"
        print filt.get_parent

    def visitFlatmap(self, flatMap):
        print "visit FlatMap\n"
        print flatMap.get_parent()

    def visitReduceByKey(self, reduceByKey):
        print "visit ReduceByKey\n"
        print reduceByKey.get_parent()

    def visitMapValue(self, mapValue):
        print "visit MapValue\n"
        print mapValue.get_parent()

    def visitJoin(self, join):
        print "visit join\n"
        print join.get_parent()

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
