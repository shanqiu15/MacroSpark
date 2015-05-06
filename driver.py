from rdd import *
from partition import *
import logging
import zerorpc
import StringIO
import pickle

import cloudpickle
import gevent

class SparkContext():

    def __init__(self, worker_list):
        #setup cluster worker connections
        self.workers  = worker_list
        self.connections = []
        for index, worker in enumerate(worker_list):
            c = zerorpc.Client(timeout=1)
            c.connect("tcp://" + worker)
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
                         "MapValue"    : self.visitMapValue,
                         "Join"        : self.visitJoin,
                         "RePartition" : self.visitRepartition,
                        }

    def _reference_counter_acc(parent_id):
        if parent_id in self.reference_counter.keys():
            self.reference_counter[parent_id] = self.reference_counter[parent_id] + 1
        else:
            self.reference_counter[parent_id] = 1

    def visit_lineage(self, rdd):
        self.last_id = 0
        for op in rdd.get_lineage():
            self.options[op.__class__.__name__](op)
            self.last_id = op.id
        self.stages.append(self.operations[self.last_id])
        print self.stages
        return self.operations

    def lineage_test(self, objstr):
        '''
        Send the rdd object from the client and generate the lineage back
        '''
        client_input = StringIO.StringIO(objstr)
        unpickler = pickle.Unpickler(client_input)
        j = unpickler.load()
        lineage = self.visit_lineage(j)
        return str(lineage)


    def execute(self):
        i = 0
        for stage in self.stages:
            for conn in self.connections:
                #?????? stage.cache() #call cache in each stage to triger the execution
                print "this is the execution for stage"
                pass
            

    def execution_acc(self, rdd_id):
        self.execution_counter 

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

        self.operations[repartition.id] = RePartition(repartition.id, self.operations[parent.id], self.workers)
        
        self.stages.append(self.operations[repartition.id])
        

    def collect(self, rdd):
        pass

    def count(self, rdd):
        pass

if __name__ == "__main__":

    # r = TextFile('myfile')
    # m = Map(r, lambda s: s.split())
    # f = Filter(m, lambda a: int(a[1]) > 2)
    # mv = MapValue(f, lambda s:s)
    # r = ReduceByKey(mv, lambda x, y: x + y)
    # z = Filter(m, lambda a: int(a[1]) < 2)
    # j = Join(z, r)
    worker_list = ["127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"]
    sc = SparkContext(worker_list)
    # sc.visit_lineage(j)
    # print sc.operations



    for conn in sc.connections:
        print conn.call_hello()
        conn.setup_worker_con(worker_list, "127.0.0.1:4242")

    logging.basicConfig()
    s = zerorpc.Server(sc)
    s.bind("tcp://0.0.0.0:4242")
    s.run()
