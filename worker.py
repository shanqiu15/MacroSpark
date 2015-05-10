#!/usr/bin/env python

import StringIO
import pickle

import cloudpickle
import gevent
import zerorpc
import sys

from rdd import *
from partition import *

# python worker 127.0.01:9001

class Worker(object):

    def __init__(self, addr):
        #gevent.spawn(self.controller)
        self.addr = addr

        #Rdd partition key value pair {rdd_id: partition object}
        #Each time when you want to get the relevent partition using self.rdd_partition[rdd_id]
        self.rdd_partition = {}
      
    def setup_worker_con(self,worker_list, driver_addr):
        self.worker_list  = worker_list
        self.worker_conn = {}
        for index, worker in enumerate(worker_list):
            if self.addr != worker:
                c = zerorpc.Client(timeout=1)
                c.connect("tcp://" + worker)
                self.worker_conn[index] = c
            else:
                self.index = index
        print "worker connection: ", self.worker_conn

        self.driver_conn = zerorpc.Client(timeout=1)
        self.driver_conn.connect("tcp://" + driver_addr)
        print "driver connection: ", self.driver_conn

    def stage_test(self, objstr):
        '''
        Send the partition object to the client and execute the clousure
        '''
        client_input = StringIO.StringIO(objstr)
        unpickler = pickle.Unpickler(client_input)
        j = unpickler.load()
        j.cache()
        return str(j.lineage)

    # def setup_repartition(self, repartition_rdd):
    #     self.repartition_rdd = repartition_rdd

    def run(self, objstr):
        input = StringIO.StringIO(objstr)
        unpickler = pickle.Unpickler(input)
        f = unpickler.load()

        #setup the connections for repartition rdd
        f.setup_connections(self.worker_conn, self.driver_conn)
        f.set_partition_index(self.index)

        #record this rdd partition and execute this clousure 
        self.rdd_partition[f.rdd_id] = f
        f.cache(self.rdd_partition)

        print "This is the caculation for ", f.rdd_id
        print f.data

        return True

    # Send the partition to driver used for collect
    def collect(self, rdd_id):
        return self.rdd_partition[rdd_id].data

    def count(self, rdd_id):
        return len(self.rdd_partition[rdd_id].data)

    def getPartition(self, rdd_id):
        return self.rdd_partition[rdd_id]

    # collect data in repartition phase
    def collect_data(self, rdd_id, split):
        self.rdd_partition[rdd_id].data = self.rdd_partition[rdd_id].data + split

    def call_hello(self):
        print "Connected Successfully"
        return "Worker " + self.addr + " runs successfully"



if __name__ == "__main__":
    worker = Worker(sys.argv[1])
    s = zerorpc.Server(worker)
    s.bind("tcp://" + sys.argv[1])
    s.run()
