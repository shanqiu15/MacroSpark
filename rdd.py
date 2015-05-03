from collections import defaultdict
from itertools import groupby

RDD_ID = 0

class Driver(object):
    def __init__(self):
        #setup cluster worker connections
        self.workers  = worker_list
        self.worker_index = worker_index
        self.connections = []
        for index, worker in enumerate(worker_list):
            if self.index != index:
                c = zerorpc.Client(timeout=1)
                c.connect("tcp://" + worker)
                self.connections.append(c)

class RDD(object):

    def __init__(self, worker_list, worker_index, rdd = None):

        #set an id for this RDD
        global RDD_ID
        RDD_ID = RDD_ID + 1
        self.id = RDD_ID

        #create new rdds using exiting rdd
        self.rdd = rdd

        #parameters used for repartion
        # self.new_rdd = None
        # self.split_result = {}
        # self.split = None
        # self._stage_index = 0

    def collect(self):
        yield ("collect", self.id)

    def map(self, func):
        print "This is a map operation"
        yield ("map", func, self.id)

    def flatmap(self, func):
        print "This is a flatmap operation"
        yield ("flatmap", func, self.id)

    def filter(self, func):
        print "This is a filter operation"
        yield ("filter", func, self.id)

    def groupByKey(self):
        print "This is a groupByKey operation"
        yield ("groupByKey", None,  )
        self.rdd = sorted(self.rdd)
        self.rdd = [(key, [i[1] for i in group]) for key, group in groupby(self.rdd, lambda x: x[0])]
        return self

    def reduceByKey(self):
        print "This is the caculation in reduceByKey"
        yield ("reduceByKey", func)

    def mapValues(self, func):
        print "This is a map value operation"
        yield ("mapValue", func)

    def join(self, rhs_rdd_id):
        print "This is an join operation"
        yield ("join", rhs_rdd_id)

    def readFile(self, filePath, start_offsize = 0, end_offize = 0):
        print "This is a read file operation"
        yield ("readFile", filePath, start_offsize, end_offize)


if __name__ == "__main__":
    # j = Join("old_1", "old_2");
    # for i in j.get():
    #     print i
    # print j.get()
    p = RDD()
    l = p.readFile('myfile')
    m = l.flatmap(lambda s: s.split())
    p = m.map(lambda s: (s, 1))
    g = p.reduceByKey()
    t = g.mapValues(lambda s: s - 4)
    x = t.getRdd()
    r = t.join(x)
    print r.getRdd()

    # f = m.rdd_filter(lambda a: int(a[1]) > 2)
    # print f.collect(), f.count()

    