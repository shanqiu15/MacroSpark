from itertools import groupby

class Partition(object):

    def __init__(self, rdd_id, partition_index = None):
        self.rdd_id = rdd_id
        self.partition_index = partition_index

    def set_partition_index(self, index):
        self.partition_index = index

    def collect(self):
        elements = []
        for element in self.get():
            elements.append(element)
        return elements

    def show(self):
        for elem in self.get():
            print elem

class FilePartition(Partition):

    def __init__(self, rdd_id, filename):
        super(FilePartition, self).__init__(rdd_id)
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

class MapPartition(Partition):

    def __init__(self, rdd_id, parent, func):
        super(MapPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self):
        print "This is the caculation in mapper"
        for element in self.parent.get():
            yield self.func(element)


class MapValuePartition(Partition):

    def __init__(self, rdd_id, parent, func):
        super(MapValuePartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self):
        print "This is the caculation in MapValuePartition"
        for key, value in self.parent.get():
            yield (key, self.func(value))

class FlatMapPartition(Partition):
    def __init__(self, rdd_id, parent, func):
        super(FlatMapPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self):
        print "This is the caculation in mapper"
        for element in self.parent.get():
            for i in self.func(element):
                yield i


class FilterPartition(Partition):
    
    def __init__(self,  rdd_id, parent, func):
        super(FilterPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self):
        print "This is the caculation in filter"
        for element in self.parent.get():
            if self.func(element):
                yield element

class GroupByKeyPartition(Partition):
    def __init__(self,  rdd_id, parent):
        super(GroupByKeyPartition, self).__init__(rdd_id)
        self.parent = parent

    def get(self):
        '''
        This function can be optimized by not creating list directly
        Should do the optimization later
        '''
        parent_rdd = [element for element in self.parent.get()]
        sorted_rdd = sorted(parent_rdd)
        group_rdd = [(key, [i[1] for i in group]) for key, group in groupby(sorted_rdd, lambda x: x[0])]
        for element in group_rdd:
            yield element


class ReduceByKeyPartition(Partition):
    def __init__(self, rdd_id, parent, func):
        super(ReduceByKeyPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self):
        grouper =  GroupByKeyPartition(-1 , parent)
        for key, group in grouper.get():
            yield (key, reduce(self.func, group))


class JoinPartition(Partition):
    def __init__(self, rdd_id, parent_1, parent_2):
        super(JoinPartition, self).__init__(rdd_id)
        self.parent_1 = parent_1
        self.parent_2 = parent_2

    def get(self):
        '''
        now we just compare each element with all the elements in the other lists
        we should optimize this function buy sorting the two list first then compare 
        one by one
        '''

        result = []
        for i in self.parent_1.get():
            for j in self.parent_2.get():
                if i[0] == j[0]:
                    print (i[0], (i[1], j[1]))
                    yield (i[0], (i[1], j[1]))

if __name__ == "__main__":

    r = FilePartition(1, 1, 'myfile')
    m = MapPartition(2, 1, r, lambda s: s.split())
    f = FilterPartition(3, 1, m, lambda a: int(a[1]) > 2)

    z = FilePartition(4 , 1, 'myfile')
    q = MapPartition(5, 1, r, lambda s: s.split())
    j = JoinPartition(6, 1, m, q)
    print j.collect()