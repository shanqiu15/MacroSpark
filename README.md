# MacroSpark

hadoop:MacroSpark hao$ python start_workers.py start

Don't start worker in the shell it will slow down the shell

Run Script mode

```
start worker:
python worker.py 127.0.0.1:9001
python worker.py 127.0.0.1:9002

start driver:
python driver.py 127.0.0.1:4242
```
Note: this will run the job in driver's main function


Run Shell mode:
```
start worker:
python worker.py 127.0.0.1:9001
python worker.py 127.0.0.1:9002

start driver:
python MacroSparkShell.py 127.0.0.1:4242
```
Note:This will start the shell mode like pyspark


Code example:
RDD transformation:
```
    r = TextFile('testFile')
    m = FlatMap(r, lambda s: s.split())
    f = Map(m, lambda a: (a, 1))
    mv = MapValue(f, lambda s:s)
    r = ReduceByKey(f, lambda x, y: x + y)
    z = Filter(m, lambda a: int(a[1]) < 2)
```
Initialize SaprkContext:
```
    worker_list = ["127.0.0.1:9001", "127.0.0.1:9002"]
    sc = SparkContext(worker_list, sys.argv[1])

    threads = [gevent.spawn(conn.setup_worker_con, worker_list, "127.0.0.1:4242") for conn in sc.connections]
    gevent.joinall(threads)
```
Execute the transformation:
```
    sc.visit_lineage(z) #z is the RDD we need to caculate
    sc.job_schedule()
    sc.collect(z)       #This is the way to collect "z" to the driver
```

