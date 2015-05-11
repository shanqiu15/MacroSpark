from driver import *
import code

if __name__ == "__main__":

    r = TextFile('/Users/mingluma/2015Spring/OS2015s/macroSpark/MacroSpark/testFile')
    m = FlatMap(r, lambda s: s.split())
    f = Map(m, lambda a: (a, 1))
    mv = MapValue(f, lambda s:s)
    r = ReduceByKey(f, lambda x, y: x + y)
    z = Filter(r, lambda a: int(a[1]) < 2)
    j = Join(f, f)

    #Setup the driver and worker
    worker_list = ["127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003", "127.0.0.1:9004"]
    #worker_list = ["127.0.0.1:9001", "127.0.0.1:9002"]
    sc = SparkContext(worker_list, sys.argv[1])
    sc.worker_setup()

    code.interact(local=globals())

