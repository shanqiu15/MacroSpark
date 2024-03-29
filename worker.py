#!/usr/bin/env python

import sys
print sys.version_info

import StringIO
import pickle
import zerorpc
from partition import *

class Worker(object):

    def __init__(self, addr):
        #gevent.spawn(self.controller)
        self.addr = addr

        #Rdd partition key value pair {rdd_id: partition object}
        #Each time when you want to get the relevent partition using self.rdd_partition[rdd_id]
        self.rdd_partition = {}

    def are_you_there(self):
        ans = {}
        ans["addr"] = self.addr
        return ans
      
    def setup_worker_con(self, worker_list, driver_addr):
        self.worker_list  = worker_list
        self.worker_conn = {}
        self.rdd_partition = {} #Clean all the cache during the worker resetting
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



        # print "This is the caculation for ", f.rdd_id
        # print f.data

    def clean(self):
        self.rdd_partition = {}

    def collect(self, rdd_id):
        '''
        :param rdd_id:
        :return:

        Send the partition data to driver used for collection
        '''
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
