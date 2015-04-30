import pickle
import StringIO

import zerorpc
import cloudpickle

from rdd import *

# r = TextFile('myfile')
# m = Map(r, lambda s: s.split())
# f = Filter(m, lambda a: int(a[1]) > 2)
# j = Join("old_1", "old_2");
class Rdd(RDDPartition):
	def __init__(self, worker_list):


class Driver(object):
    def __init__(self, worker_list):
        self.worker_list = worker_list
        self.connections = []
        for index, worker in enumerate(worker_list):
            c = zerorpc.Client(timeout=1)
            c.connect("tcp://" + worker)
            self.connections.append(c)
            #c.set_cluster_info(self.worker_list, index)
    
    def readFile(self, filePath):
        for conn in self.connections:
            conn.readFile(filePath)

    def showPartition(self):
        for conn in self.connections:
            conn.showPartition()

    def linage(self):
        r = yield
        m = r.rdd_map(lambda s: s.split())
        f = m.rdd_filter(lambda a: int(a[1]) > 2)

# output = StringIO.StringIO()
# pickler = cloudpickle.CloudPickler(output)
# pickler.dump(f)
# # pickler.dump(j)
# objstr = output.getvalue()

# c = zerorpc.Client()
# c.connect("tcp://127.0.0.1:4242")

# print c.run(objstr)


if __name__ == "__main__":
    worker_list = ["127.0.0.1:9001", "127.0.0.1:9002"]
    driver = Driver(worker_list)
    driver.readFile("myfile")
    driver.showPartition()
