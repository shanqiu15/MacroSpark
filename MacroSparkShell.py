__author__ = 'hao'
from rdd import *
from partition import *
from gevent.pool import Group
import logging
import zerorpc
import StringIO
import pickle
import sys

import cloudpickle
import gevent
import code

class SparkContext():

    def __init__(self, worker_list, addr):
        #setup cluster worker connections
        self.workers  = worker_list
        # self.evt = Event()
        self.addr = addr
        self.connections = []
        for index, worker in enumerate(worker_list):
            c = zerorpc.Client(timeout=1)
            c.connect("tcp://" + worker)
            c.setup_worker_con(worker_list, self.addr)
            self.connections.append(c)

        #key: rdd_id, value: partition operation object
        self.operations = {}
        self.stages = []

        # map the inputs to the function blocks
        self.options = { "TextFile"    : self.visitTextFile,
                         "Map"         : self.visitMap,
                         "Filter"      : self.visitFilter,
                         "FlatMap"     : self.visitFlatmap,
                         "ReduceByKey" : self.visitReduceByKey,
                         "GroupByKey"  : self.visitGroupByKey,
                         "MapValue"    : self.visitMapValue,
                         "Join"        : self.visitJoin,
                         "RePartition" : self.visitRepartition,
                        }

    def visit_lineage(self, rdd):

        #initialize repartition list and the stage list
        self.repartition_stages = []
        self.stages = []
        # self.evt.set()

        self.last_id = 0
        for op in rdd.get_lineage():
            self.options[op.__class__.__name__](op)
            self.last_id = op.id

        #add the last operation to the stages list
        self.stages.append(self.operations[self.last_id])
        print "This is all the stages: ", self.stages
        return self.operations

    def execute_lineage(self, objstr):
        '''
        Send the rdd object from the client and generate the lineage back
        '''
        # self.evt.set()
        client_input = StringIO.StringIO(objstr)
        unpickler = pickle.Unpickler(client_input)
        j = unpickler.load()
        lineage = self.visit_lineage(j)
        return str(lineage)




    def execute(self, stage, conn):
        # self.evt.wait()
        output = StringIO.StringIO()
        pickler = cloudpickle.CloudPickler(output)
        pickler.dump(stage)
        objstr = output.getvalue()
        return conn.run(objstr)

    def job_schedule(self):
        '''
        Execute the stage one by one
        Output the RDD if collect was true
        '''
        for stage in self.stages:
            self.rdd_data = [] #record the rdd collect data
            self.counter = 0 # a list record the count for each partition
            threads = [gevent.spawn(self.execute, stage, conn) for conn in self.connections]
            gevent.joinall(threads)
            if stage.collect:
                for conn in self.connections:
                    self.rdd_data = self.rdd_data + conn.collect(stage.rdd_id)
                print self.rdd_data
                stage.collect = False

            if stage.count:
                for conn in self.connections:
                    self.counter = self.counter + conn.count(stage.rdd_id)
                print self.counter
                stage.count = False

    def collect(self, rdd):
        rdd.collect = True
        self.visit_lineage(rdd)
        self.job_schedule()

    def worker_setup(self):
        '''
        reset up worker connection and other configuration after worker failure
        '''
        pass

    def _set_collect_count(self, rdd):
        if rdd.collect:
            self.operations[rdd.id].collect = True
        if rdd.count:
            self.operations[rdd.id].count = True


    # define the function blocks
    def visitTextFile(self, textfile):
        # print "visit TestFile %d", textfile.id
        # print textfile, "\n"
        # print textfile.__class__.__name__
        # print textfile.filePath

        self.operations[textfile.id] = FilePartition(textfile.id, textfile.filePath, len(self.workers))
        self._set_collect_count(textfile)


    def visitMap(self, mapper):
        # print "visit Map %d", mapper.id
        # print mapper, "\n"

        parent = mapper.get_parent()
        self.operations[mapper.id] = MapPartition(mapper.id, self.operations[parent.id], mapper.func)
        self._set_collect_count(mapper)


    def visitFilter(self, filt):
        # print "visit Filter %d", filt.id
        # print filt, "\n"

        parent = filt.get_parent()
        self.operations[filt.id] = FilterPartition(filt.id, self.operations[parent.id], filt.func)
        self._set_collect_count(filt)


    def visitFlatmap(self, flatMap):
        # print "visit FlatMap %d", flatMap.id
        # print flatMap, "\n"

        parent = flatMap.get_parent()
        self.operations[flatMap.id] = FlatMapPartition(flatMap.id, self.operations[parent.id], flatMap.func)
        self._set_collect_count(flatMap)


    def visitGroupByKey(self, groupByKey):
        # print "visit GroupByKey %d", groupByKey.id
        # print groupByKey, "\n"

        parent = groupByKey.get_parent()
        self.operations[groupByKey.id] = GroupByKeyPartition(groupByKey.id, self.operations[parent.id])
        self._set_collect_count(groupByKey)


    def visitReduceByKey(self, reduceByKey):
        # print "visit ReduceByKey %d", reduceByKey.id
        # print reduceByKey, "\n"

        parent = reduceByKey.get_parent()
        self.operations[reduceByKey.id] = ReduceByKeyPartition(reduceByKey.id, self.operations[parent.id], reduceByKey.func)
        self._set_collect_count(reduceByKey)


    def visitMapValue(self, mapValue):
        # print "visit MapValue %d", mapValue.id
        # print mapValue, "\n"

        parent = mapValue.get_parent()
        self.operations[mapValue.id] = MapValuePartition(mapValue.id, self.operations[parent.id], mapValue.func)
        self._set_collect_count(mapValue)


    def visitJoin(self, join):
        # print "visit join %d",join.id
        # print join,"\n"

        parent = join.get_parent()
        self.operations[join.id] = JoinPartition(join.id, self.operations[parent[0].id],self.operations[parent[1].id])
        self._set_collect_count(join)


    def visitRepartition(self, repartition):
        # print "visit repartition %d",repartition.id
        # print repartition, "\n"

        parent = repartition.get_parent()
        self.stages.append(self.operations[parent.id])
        self.operations[repartition.id] = RePartition(repartition.id, self.operations[parent.id], len(self.workers))
        self.stages.append(self.operations[repartition.id])
        self._set_collect_count(repartition)

    def count(self, rdd):
        pass

if __name__ == "__main__":

    r = TextFile('testFile')
    m = FlatMap(r, lambda s: s.split())
    f = Map(m, lambda a: (a, 1))
    mv = MapValue(f, lambda s:s)
    r = ReduceByKey(f, lambda x, y: x + y)
    z = Filter(r, lambda a: int(a[1]) < 2)
    j = Join(f, f)
    # j.rdd_collect()
    # j.rdd_count()



    #Setup the driver and worker
    # worker_list = ["127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003", "127.0.0.1:9004"]
    worker_list = ["127.0.0.1:9001", "127.0.0.1:9002"]
    sc = SparkContext(worker_list, sys.argv[1])

    threads = [gevent.spawn(conn.setup_worker_con, worker_list, "127.0.0.1:4242") for conn in sc.connections]
    gevent.joinall(threads)

    #the process of caculate RDD "z"
    # sc.visit_lineage(j)
    # sc.job_schedule()

    code.interact(local=globals())

