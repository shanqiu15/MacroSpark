from rdd import *
from partition import *
import zerorpc
import StringIO
import cloudpickle

def word_count(filename):
    textfile = TextFile(filename)
    f = FlatMap(textfile, lambda x: x.split())
    m = Map(f, lambda x: (x, 1))
    r = ReduceByKey(m, lambda x, y: x + y)
    r.rdd_collect()#set the collect to be true then we will collect all the data to driver

    output = StringIO.StringIO()
    pickler = cloudpickle.CloudPickler(output)
    pickler.dump(r)
    objstr = output.getvalue()

    c = zerorpc.Client()
    c.connect("tcp://127.0.0.1:4242")
    c.execute_lineage(objstr)

def log_query(filename, keyword):
    logfile = TextFile(filename)
    f = Filter(logfile, lambda x: (keyword in x))
    f.rdd_collect()

    output = StringIO.StringIO()
    pickler = cloudpickle.CloudPickler(output)
    pickler.dump(f)
    objstr = output.getvalue()

    c = zerorpc.Client()
    c.connect("tcp://127.0.0.1:4242")
    c.execute_lineage(objstr)


if __name__ == "__main__":
    # word_count("/Local/Users/hao/Desktop/MacroSpark/testFile")
    log_query("/Local/Users/hao/Desktop/MacroSpark/sample.log", "error")
