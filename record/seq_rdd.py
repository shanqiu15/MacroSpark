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
            print line
            yield line

class Map(RDD):

    def __init__(self, parent, func):
        self.parent = parent
        self.func = func

    def get(self):
        print "This is the caculation in mapper"
        for element in self.parent.get():
            yield self.func(element)

class Filter(RDD):
    
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func

    def get(self):
        print "This is the caculation in filter"
        for element in self.parent.get():
            if self.func(element):
                yield element

class Join(RDD):
    def __init__(self, parent_1, parent_2):
        self.parent_1 = parent_1
        self.parent_2 = parent_2

    def get(self):
        for elem in self.yield_get():
            print elem

    def yield_get(self):
        print "in the get of join again"
        repartition_generator = self.repartition(self.parent_1)
        repartition_generator.send(None)
        ################################
        ######repartition process#######
        ################################
        new_rdd = "This is the new rdd_1"
        self.parent_1 = repartition_generator.send(new_rdd)
        yield self.parent_1

        repartition_generator = self.repartition(self.parent_2)
        repartition_generator.send(None)
        ################################
        ######repartition process#######
        ################################
        new_rdd = "This is the new rdd_2"
        self.parent_2 = repartition_generator.send(new_rdd)
        yield self.parent_2

    def repartition(self, rdd):
        rdd = yield
        print rdd
        yield rdd

if __name__ == "__main__":
    # j = Join("old_1", "old_2");
    # for i in j.get():
    #     print i
    # print j.get()
    r = TextFile('myfile')
    print r.collect()
    # m = Map(r, lambda s: s.split())
    # f = Filter(m, lambda a: int(a[1]) > 2)
    # print f.collect(), f.count()

    