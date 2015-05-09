from rdd import *
from partition import *
from gevent.event import Event
import logging
import zerorpc
import StringIO
import pickle
import sys

import cloudpickle
import gevent

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

        #The cached intermediate rdd data {rdd_id: data}
        self.caches = {}

        #record how many rdds depend on this rdd, when one rdd's reference_counter 
        #is zero the cached data in "caches" can be garbage collected {rdd_id: counter}
        self.reference_counter = {}

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

    def lineage_test(self, objstr):
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

        # #process for repartition#
        # if stage.rdd_id in self.repartition_stages:
        #     self.is_reparted[stage.id] = False
        #     while(not self.is_reparted[stage.id]):
        #         sleep()
            

    def execution_acc(self, rdd_id):
        pass

    # def collect(rdd):
    #     '''
    #     An rdd object which need to be collected
    #     '''
    #     self.operations[rdd.id]


            

    # define the function blocks
    def visitTextFile(self, textfile):
        print "visit TestFile %d", textfile.id
        print textfile, "\n"
        print textfile.__class__.__name__
        print textfile.filePath

        self.operations[textfile.id] = FilePartition(textfile.id, textfile.filePath, len(self.workers))
        # for i in range(len(worker_list)):
        #     r[i] = FilePartition(textfile, textfile.id, i)


    def visitMap(self, mapper):
        print "visit Map %d", mapper.id
        print mapper, "\n"
        parent = mapper.get_parent()
        # self._reference_counter_acc(parent.id)
        self.operations[mapper.id] = MapPartition(mapper.id, self.operations[parent.id], mapper.func)

        # return MapPartition(rdd_id, parent, func)
        # for i in range(len(worker_list)):
        #     m[i] = 

    def visitFilter(self, filt):
        print "visit Filter %d", filt.id
        print filt, "\n"
        parent = filt.get_parent()
        # self._reference_counter_acc(parent.id)
        self.operations[filt.id] = FilterPartition(filt.id, self.operations[parent.id], filt.func)

    def visitFlatmap(self, flatMap):
        print "visit FlatMap %d", flatMap.id
        print flatMap, "\n"
        parent = flatMap.get_parent()
        # self._reference_counter_acc(parent.id)
        self.operations[flatMap.id] = FlatMapPartition(flatMap.id, self.operations[parent.id], flatMap.func)

    def visitGroupByKey(self, groupByKey):
        print "visit GroupByKey %d", groupByKey.id
        print groupByKey, "\n"

        parent = groupByKey.get_parent()
        self.operations[groupByKey.id] = GroupByKeyPartition(groupByKey.id, self.operations[parent.id])


    def visitReduceByKey(self, reduceByKey):
        print "visit ReduceByKey %d", reduceByKey.id
        print reduceByKey, "\n"

        parent = reduceByKey.get_parent()

        #self.operations[parent.id].cache()
        #self._reference_counter_acc(parent.id)

        self.operations[reduceByKey.id] = ReduceByKeyPartition(reduceByKey.id, self.operations[parent.id], reduceByKey.func)

    def visitMapValue(self, mapValue):
        print "visit MapValue %d", mapValue.id
        print mapValue, "\n"
        parent = mapValue.get_parent()
        # self._reference_counter_acc(parent.id)
        self.operations[mapValue.id] = MapValuePartition(mapValue.id, self.operations[parent.id], mapValue.func)

    def visitJoin(self, join):
        print "visit join %d",join.id
        print join,"\n"
        parent = join.get_parent()

        # self._reference_counter_acc(parent[0].id)
        # self._reference_counter_acc(parent[1].id) 
        self.operations[join.id] = JoinPartition(join.id, self.operations[parent[0].id],self.operations[parent[1].id])

    def visitRepartition(self, repartition):
        print "visit repartition %d",repartition.id
        print repartition, "\n"
        parent = repartition.get_parent()

        self.stages.append(self.operations[parent.id])

        self.operations[repartition.id] = RePartition(repartition.id, self.operations[parent.id], len(self.workers))
        self.stages.append(self.operations[repartition.id])

    def collect(self, rdd):
        pass

    def count(self, rdd):
        pass

if __name__ == "__main__":

    r = TextFile('testFile')
    m = FlatMap(r, lambda s: s.split())
    f = Map(m, lambda a: (a, 1))
    #mv = MapValue(f, lambda s:s)
    #r = ReduceByKey(f, lambda x, y: x + y)
    #z = Filter(m, lambda a: int(a[1]) < 2)
    j = Join(f, f)



    #Setup the driver and worker
    # worker_list = ["127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003", "127.0.0.1:9004"]
    worker_list = ["127.0.0.1:9001", "127.0.0.1:9002"]
    sc = SparkContext(worker_list, sys.argv[1])

    threads = [gevent.spawn(conn.setup_worker_con, worker_list, "127.0.0.1:4242") for conn in sc.connections]
    gevent.joinall(threads)



    # for conn in sc.connections:
    #     print conn.call_hello()
    #     conn.setup_worker_con(worker_list, "127.0.0.1:4242")
    #     conn.setup_repartition(sc.repartition_stages)


    #the process of caculate RDD "z"
    sc.visit_lineage(j)
    # threads = [gevent.spawn(conn.setup_conn, sc.repartition_stages) for conn in sc.connections]
    # gevent.joinall(threads)
    #sc.test_execute()
    for stage in sc.stages:
        threads = [gevent.spawn(sc.execute, stage, conn) for conn in sc.connections]
        gevent.joinall(threads)
        # # self.evt.set()
        # if statge.is_repartition():
        #     # self.evt.clear()
        #     threads = [gevent.spawn(sc.execute, stage, conn) for conn in sc.connections]
        #     gevent.joinall(threads)



    # print sc.operations



    
    logging.basicConfig()
    s = zerorpc.Server(sc)
    s.bind("tcp://" + sys.argv[1])
    s.run()
