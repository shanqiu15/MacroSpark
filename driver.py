from rdd import *

class SparkContext():
    def __init__(self):
    # def __init__(self, worker_list):
    #     #setup cluster worker connections
    #     self.workers  = worker_list
    #     self.connections = []
    #     for index, worker in enumerate(worker_list):
    #         if self.index != index:
    #             c = zerorpc.Client(timeout=1)
    #             c.connect("tcp://" + worker)
    #             self.connections.append(c)

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
            self.options[op.__class__.__name__]()

    # define the function blocks
    def visitTextFile(self):
        print "visit TestFile.\n"

    def visitMap(self):
        print "visit Map\n"

    def visitFilter(self):
        print "visit Filter\n"

    def visitFlatmap(self):
        print "visit FlatMap\n"

    def visitReduceByKey(self):
        print "visit ReduceByKey\n"

    def visitMapValue(self):
        print "visit MapValue\n"

    def visitJoin(self):
        print "visit join\n"



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
