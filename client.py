from rdd import *
from partition import *
import zerorpc
import StringIO
import cloudpickle

def word_count(filename):
    textfile = TextFile(filename)
    flat = FlatMap(textfile, lambda x: x.split())
    map = Map(flat, lambda x: (x, 1))
    red = ReduceByKey(map, lambda x, y: x + y)
    red.rdd_collect()#set the collect to be true then we will collect all the data to driver

    wc_output = StringIO.StringIO()
    pickler = cloudpickle.CloudPickler(wc_output)
    pickler.dump(red)
    return wc_output.getvalue()



def log_query(filename, keyword):
    logfile = TextFile(filename)
    fil = Filter(logfile, lambda x: (keyword in x))
    fil.rdd_collect()

    log_output = StringIO.StringIO()
    pickler = cloudpickle.CloudPickler(log_output)
    pickler.dump(fil)
    return log_output.getvalue()



if __name__ == "__main__":
    c = zerorpc.Client()
    c.connect("tcp://127.0.0.1:4242")

    # c.execute_lineage(word_count("/Local/Users/hao/Desktop/MacroSpark/input/testFile"))
    # c.execute_lineage(log_query("/Local/Users/hao/Desktop/MacroSpark/input/sample.log", "error"))
