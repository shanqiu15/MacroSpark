from itertools import groupby
import time

class RDD(object):

    def __init__(self):
        pass

    #each partition will call yield and seperate the linage to different stages
    def linage(self):
        for tmp_rdd in self.get():
            return tmp_rdd
            #
            #call the repartition function
            #

    def collect(self):
        elements = []
        for element in self.get():
            elements.append(element)
        return elements

    def count(self):
        return len(self.collect())

class TextFile(RDD):

    def __init__(self, filename):
        self.filename = filename
        self.lines = None

    def get(self):
        print "This is the get in TextFile"
        if not self.lines:
            f = open(self.filename)
            self.lines = f.readlines()
            f.close()
    
        for line in self.lines:
            yield line

class Map(RDD):

    def __init__(self, parent, func):
        self.parent = parent
        self.func = func

    def get(self):
        for element in self.parent.get():
            yield self.func(element)

class FlatMap(RDD):
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func

    def get(self):
        for element in self.parent.get():
            for i in self.func(element):
                yield i

class ReduceByKey(RDD):
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func

    def get(self):
        parent_rdd = [element for element in self.parent.get()]
        sorted_rdd = sorted(parent_rdd)
        group_data = [(key, [i[1] for i in group]) for key, group in groupby(sorted_rdd, lambda x: x[0])]
        self.data = [(key, reduce(self.func, group)) for key, group in group_data]
        for i in self.data:
            print i


if __name__ == "__main__":
    start_time = time.time()
    r = TextFile('shell_code')
    f = FlatMap(r, lambda s: s.split())
    m = Map(f, lambda s: (s,1))
    r = ReduceByKey(m, lambda x, y: x + y)
    r.get()
    print "running time is ", time.time() - start_time


    