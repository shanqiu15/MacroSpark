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

    def __init__(self):
        #gevent.spawn(self.controller)
        self.rddPartition = None

        #Rdd partition key value pair {rdd_id: partition object}
        #Each time when you want to get the relevent partition using self.rdd_partition[rdd_id]
        self.rdd_partition = {}
      

    def run(self, objstr):
        input = StringIO.StringIO(objstr)
        unpickler = pickle.Unpickler(input)
        f = unpickler.load()

        #record this rdd partition and execute this clousure
        self.rdd_partition[f.rdd_id] = f
        f.cache()

    def collect(self, rdd_id):
        return self.rdd_partition[rdd_id].data

    def getPartition(self, rdd_id):
        return self.rdd_partition[rdd_id]



if __name__ == "__main__":
    s = zerorpc.Server(Worker())
    s.bind("tcp://" + sys.argv[1])
    s.run()
