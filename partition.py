from collections import defaultdict
from itertools import groupby


class RDD(object):
    def __init__(self, rdd, worker_list):
        self.rdd = rdd
        self.workers  = enumerate(worker_list)
    # def 

class RDDPartition(object):

    def __init__(self, partition = None, partition_index = None):
        self.partition = partition
        self.partition_index = partition_index
        # self.workers  = enumerate(worker_list)
        # self.addr = worker_addr
    
    def readFile(self, filePath):
        #######################
        #need to change to split the file while reading 
        #######################
        f = open(filePath)
        lines = f.readlines()
        f.close()
        self.partition = lines
        return self


    def collect(self):
        pass

    def count(self):
        return len(self.collect())    

    #each partition will call yield and seperate the linage to different stages
    def linage(self):
        for tmp_rdd in self.get():
            return tmp_rdd
            #
            #call the repartition function
            #

    def getPartition(self):
        return self.partition

    def partition_map(self, func):
        print "This is the caculation in mapper"
        self.partition = [func(elem) for elem in self.partition]
        return self

    def partition_flatmap(self, func):
        print "This in the flatmap"
        tem = []
        for elem in self.partition:
            tem = tem + func(elem)
        self.partition =  tem
        return self

    def partition_filter(self, func):
        print "This is the caculation in filter"
        self.partition = [elem for elem in self.partition if func(elem)]
        return self

    def partition_groupByKey(self):
        print "This is the caculation in groupByKey"
        ################Debugging lines################
        #self.partition = self.partition + self.partition + self.partition
        ###############################################
        self.partition = sorted(self.partition)
        self.partition = [(key, [i[1] for i in group]) for key, group in groupby(self.partition, lambda x: x[0])]
        return self

    def partition_reduceByKey(self):
        print "This is the caculation in reduceByKey"
        self.partition = self.partition_groupByKey().getPartition()
        self.partition = [(key, sum(group)) for key, group in self.partition]
        return self

    def partition_mapValues(self, func):
        print "This is the caculation in mapValues"
        self.partition = [(key, func(value)) for key, value in self.partition]
        return self

    def partition_join(self, join_partition):
        result = []
        for i in self.getPartition():
            for j in join_partition:
                if i[0] == j[0]:
                    result.append((i[0], (i[1], j[1])))
        self.partition = result
        return self



if __name__ == "__main__":
    # j = Join("old_1", "old_2");
    # for i in j.get():
    #     print i
    # print j.get()
    p = RDDPartition()
    l = p.readFile('myfile')
    m = l.partition_flatmap(lambda s: s.split())
    p = m.partition_map(lambda s: (s, 1))
    g = p.partition_reduceByKey()
    t = g.partition_mapValues(lambda s: s - 4)
    x = t.getPartition()
    r = t.partition_join(x)
    print r.getPartition()
    # f = m.rdd_filter(lambda a: int(a[1]) > 2)
    # print f.collect(), f.count()

    