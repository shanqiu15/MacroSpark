from collections import defaultdict
from itertools import groupby

RDD_INDEX = 0

class RDD(object):

    def __init__(self, rdd_index, worker_list, worker_index, rdd = None):
        self.rdd_index = rdd_index
        self.rdd = rdd
        self.new_rdd = None
        self.split_result = {}
        self.split = None
        self._stage_index = 0
        self.workers  = worker_list
        self.index = worker_index
        self.connections = []
        for index, worker in enumerate(worker_list):
            if self.index != index:
                c = zerorpc.Client(timeout=1)
                c.connect("tcp://" + worker)
                self.connections.append(c)

    def collect(self):
        pass

    def count(self):
        return len(self.collect())  
    
    def readFile(self, rdd_index, filePath, start_offsize = 0, end_offize = 0):
        #######################
        #need to change to split the file while reading 
        #######################
        f = open(filePath)
        lines = f.readlines()
        f.close()
        self.rdd = lines
        return self
  

    def getRdd(self):
        return self.rdd

    def map(self, func):
        print "This is the caculation in mapper"
        self.rdd = [func(elem) for elem in self.rdd]
        return self

    def flatmap(self, func):
        print "This in the flatmap"
        tem = []
        for elem in self.rdd:
            tem = tem + func(elem)
        self.rdd =  tem
        return self

    def filter(self, func):
        print "This is the caculation in filter"
        self.rdd = [elem for elem in self.rdd if func(elem)]
        return self

    def groupByKey(self):
        print "This is the caculation in groupByKey"
        self.get_newRDD()
        #*****************Repartition*****************#
        self.split = self.split(self.rdd)
        self.rdd = yield self.split
        ################Debugging lines################
        #self.rdd = self.rdd + self.rdd + self.rdd
        ###############################################
        self.rdd = sorted(self.rdd)
        self.rdd = [(key, [i[1] for i in group]) for key, group in groupby(self.rdd, lambda x: x[0])]
        return self

    def reduceByKey(self):
        print "This is the caculation in reduceByKey"
        self.rdd = self.groupByKey().getRdd()
        self.rdd = [(key, sum(group)) for key, group in self.rdd]
        return self

    def mapValues(self, func):
        print "This is the caculation in mapValues"
        self.rdd = [(key, func(value)) for key, value in self.rdd]
        return self

    def join(self, join_rdd):
        self.get_newRDD()
        result = []
        for i in self.getRdd():
            for j in join_rdd:
                if i[0] == j[0]:
                    result.append((i[0], (i[1], j[1])))
        self.rdd = result
        return self

    def get_newRDD(self):
        repartition_generator = self._do_repartition()
        repartition_generator.send(None)
        #######
        #split_result = {1:[(k,v),(k,v)], 2:[(k,v),(k,v)], 3:[(k,v),(k,v)]}
        ######
        self.split_result = {}
        for k, v in self.rdd:
            key = hash(k) % len(self.worker_list)
            if key in split_result:
                self.split_result[key].append((k, v))
            else:
                self.split_result[key] = [(k, v)]

        self.new_rdd = split_result[self.index] #initialize the new RDD

        #^^^^^^Call server to start the repartion process^^^^^^^#
        # Call server, server will block the request untill all the worker are ready
        ##########^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^##########
        self.get_new_rdd()

        #place to do the repartition
        #new_rdd is the new generated rdd_partition
        #######
        repartition_generator.send(self.new_rdd)
        self.new_rdd = None
        self.split_result = None

    def get_new_rdd(self):
        '''
        Driver will call this function to let the current worker
        get the left repartion data from other wokers
        '''
        for con in self.connections:
            self.new_rdd = self.new_rdd + con.get_new_partition(self.rdd_index, self.stage_index)

    def _do_repartition(self):
        '''
        The first yield will yield the current _stage_index
        The second yield will yield the next _stage_index
        '''
        self.rdd = yield
        #print self.stage_index, self.rdd
        self._stage_index = self._stage_index + 1
        yield

    def get_new_partition(self, rdd_index, stage_index):
        '''
        we need an rdd dict to specify the different rdd
        '''
        pass



class Partition(RDD):
    def __init__(self, rdd_index, worker_list, worker_index, rdd = None):
        self.worker_index = worker_index

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

    