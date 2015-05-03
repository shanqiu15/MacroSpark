import StringIO
import pickle

import cloudpickle
import gevent
import zerorpc
import sys

from rdd import *

# python worker 127.0.01:9001

class Worker(object):

    def __init__(self):
        #gevent.spawn(self.controller)
        self.rddPartition = None
        

    def run(self, objstr):
        input = StringIO.StringIO(objstr)
        unpickler = pickle.Unpickler(input)
        f = unpickler.load()
        return str(f.lines)

    def readFile(self, filePath):
        #######################
        #need to change to split the file while reading 
        #######################
        f = open(filePath)
        lines = f.readlines()
        f.close()
        self.rddPartition = RDDPartition(lines)

    def getPartition(self):
        return self.rddPartition

    def showPartition(self):
        for i in self.rddPartition.getRDD():
            print i

    def set_cluster_info(self, worker_list, index):
        self.worker_list = worker_list
        self.index = index
        self.worker_index = dict(enumerate(worker_list))


if __name__ == "__main__":
    s = zerorpc.Server(Worker())
    s.bind("tcp://" + sys.argv[1])
    s.run()
