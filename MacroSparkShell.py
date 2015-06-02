from driver import *
import code

if __name__ == "__main__":

    #Setup the driver and worker
    worker_list = ["127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003", "127.0.0.1:9004"]
    sc = SparkContext(worker_list, sys.argv[1])
    sc.worker_setup()

    #Start the interactive shell
    code.interact(local=globals())

