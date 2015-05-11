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
        self.worker_list = worker_list # worker list in the config file
        self.workers  = [] # alive workers
        self.addr = addr
        self.connections = []

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

    def _get_live_workers(self):
        '''Get all alive workers'''
        logging.debug('Start getting workers ')
        self.workers = []
        self.connections = []
        for index, worker in enumerate(self.worker_list):
            c = zerorpc.Client(timeout=1)
            c.connect("tcp://" + worker)
            try:
                c.are_you_there()
                self.connections.append(c)
                self.workers.append(worker)
            except zerorpc.TimeoutExpired:
                continue

    def worker_setup(self):
        #Get the live workers
        self._get_live_workers()
        if len(self.workers) == 0:
            print "No workers running. Please check your servers!!!"
            sys.exit()

        print "Setup process succeed  :-)"
        print "New worker list:"
        for i in self.workers:
            print i
        threads = [gevent.spawn(conn.setup_worker_con, self.workers, self.addr) for conn in self.connections]
        gevent.joinall(threads)
        print "Now you can run your Spark jobs."
        logging.debug("Alive workers: %s", tuple(self.workers))


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
        Receive the rdd object from the client and generate the lineage back
        '''
        client_input = StringIO.StringIO(objstr)
        unpickler = pickle.Unpickler(client_input)
        j = unpickler.load()
        self.visit_lineage(j)
        self.job_schedule()


    def __execute(self, stage, conn):
        '''
        Send the stage task to worker and execute this stage in the worker
        '''
        output = StringIO.StringIO()
        pickler = cloudpickle.CloudPickler(output)
        pickler.dump(stage)
        objstr = output.getvalue()
        return conn.run(objstr)

    def job_schedule(self):
        '''
        Execute the stage one by one
        Output the result based on the collect and count flag
        '''
        for stage in self.stages:
            self.rdd_data = [] #record the rdd collect data
            self.counter = 0   #The number of the result RDD
            threads = [gevent.spawn(self.__execute, stage, conn) for conn in self.connections]
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
        '''
        :param rdd:
        :return:

        Usage:
        sc = SparkContext(...)
        sc.collect(rdd)
        '''
        rdd.collect = True
        self.visit_lineage(rdd)
        self.job_schedule()

    def count(self, rdd):
        '''
        :param rdd:
        :return:

        Usage:
        sc = SparkContext(...)
        sc.count(rdd)
        '''
        rdd.count = True
        self.visit_lineage(rdd)
        self.job_schedule()

    def _set_collect_count(self, rdd):
        '''
        :param rdd:
        :return: None
        Set the collect and count flag for the partition operations
        '''
        if rdd.collect:
            self.operations[rdd.id].collect = True
        if rdd.count:
            self.operations[rdd.id].count = True


    # Visit functions visit the RDD and convert the rdd transformation to
    # Partition Operations
    def visitTextFile(self, textfile):
        self.operations[textfile.id] = FilePartition(textfile.id, textfile.filePath, len(self.workers))
        self._set_collect_count(textfile)


    def visitMap(self, mapper):
        parent = mapper.get_parent()
        self.operations[mapper.id] = MapPartition(mapper.id, self.operations[parent.id], mapper.func)
        self._set_collect_count(mapper)


    def visitFilter(self, filt):
        parent = filt.get_parent()
        self.operations[filt.id] = FilterPartition(filt.id, self.operations[parent.id], filt.func)
        self._set_collect_count(filt)


    def visitFlatmap(self, flatMap):
        parent = flatMap.get_parent()
        self.operations[flatMap.id] = FlatMapPartition(flatMap.id, self.operations[parent.id], flatMap.func)
        self._set_collect_count(flatMap)


    def visitGroupByKey(self, groupByKey):
        parent = groupByKey.get_parent()
        self.operations[groupByKey.id] = GroupByKeyPartition(groupByKey.id, self.operations[parent.id])
        self._set_collect_count(groupByKey)


    def visitReduceByKey(self, reduceByKey):
        parent = reduceByKey.get_parent()
        self.operations[reduceByKey.id] = ReduceByKeyPartition(reduceByKey.id, self.operations[parent.id], reduceByKey.func)
        self._set_collect_count(reduceByKey)


    def visitMapValue(self, mapValue):
        parent = mapValue.get_parent()
        self.operations[mapValue.id] = MapValuePartition(mapValue.id, self.operations[parent.id], mapValue.func)
        self._set_collect_count(mapValue)


    def visitJoin(self, join):
        parent = join.get_parent()
        self.operations[join.id] = JoinPartition(join.id, self.operations[parent[0].id],self.operations[parent[1].id])
        self._set_collect_count(join)


    def visitRepartition(self, repartition):
        parent = repartition.get_parent()
        self.stages.append(self.operations[parent.id])
        self.operations[repartition.id] = RePartition(repartition.id, self.operations[parent.id], len(self.workers))
        self.stages.append(self.operations[repartition.id])
        self._set_collect_count(repartition)

if __name__ == "__main__":

    r = TextFile('testFile')
    m = FlatMap(r, lambda s: s.split())
    f = Map(m, lambda a: (a, 1))
    #mv = MapValue(f, lambda s:s)
    #r = ReduceByKey(f, lambda x, y: x + y)
    #z = Filter(m, lambda a: int(a[1]) < 2)
    j = Join(f, f)
    j.rdd_count()

    #Setup the driver and worker
    worker_list = ["127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003", "127.0.0.1:9004"]
    # worker_list = ["127.0.0.1:9001", "127.0.0.1:9002"]
    sc = SparkContext(worker_list, sys.argv[1])
    sc.worker_setup()

    #the process of caculate RDD "z"
    sc.visit_lineage(j)
    sc.job_schedule()

    logging.basicConfig()

    s = zerorpc.Server(sc)
    s.bind("tcp://" + sys.argv[1])
    s.run()
