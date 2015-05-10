from rdd import *
from partition import *
import zerorpc
import StringIO
import pickle

import cloudpickle
import gevent


r = TextFile('myfile')
m = Map(r, lambda s: s.split())
f = Filter(m, lambda a: int(a[1]) > 2)
mv = MapValue(f, lambda s:s)
r = ReduceByKey(mv, lambda x, y: x + y)
z = Filter(m, lambda a: int(a[1]) < 2)
j = Join(z, r)
j.collect()#set the collect to be true then we will collect all the data to driver

output = StringIO.StringIO()
pickler = cloudpickle.CloudPickler(output)
pickler.dump(j)
objstr = output.getvalue()

c = zerorpc.Client()
c.connect("tcp://127.0.0.1:4242")
a = c.execute_lineage(objstr)
print a

