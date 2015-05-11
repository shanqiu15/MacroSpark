from itertools import groupby

class Partition(object):

    def __init__(self, rdd_id, partition_index = None):
        self.rdd_id = rdd_id
        self.partition_index = partition_index
        self.data = []
        self.is_cached = False
        self.collect = False
        self.count = False

    def set_partition_index(self, index):
        self.partition_index = index
        if self.parent:
            self.parent.set_partition_index(index)

    def setup_connections(self, worker_conn, driver_conn):
        self.driver_conn = driver_conn
        self.worker_conn = worker_conn

    def is_repartition(self):
        return False


class FilePartition(Partition):

    def __init__(self, rdd_id, filename, worker_num):
        super(FilePartition, self).__init__(rdd_id)
        self.filename = filename
        self.partition_num = worker_num
        self.parent = None

    def get(self, rdd_partition = None):
        if not self.data:
            f = open(self.filename)
            self.data = f.readlines()
            f.close()
    
        for line in self.data:
            yield line

    def cache(self, rdd_partition = None):
        f = open(self.filename)
        self.data = f.readlines()
        f.close()
        self.is_cached = True
        # print self.data
        # print "Cached the result for FilePartition"


class MapPartition(Partition):

    def __init__(self, rdd_id, parent, func):
        super(MapPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self, rdd_partition = None):
        if self.rdd_id not in rdd_partition:
            if self.is_cached:
                for element in self.data:
                    yield element
            else:
                for element in self.parent.get(rdd_partition):
                    yield self.func(element)
        else:
            for element in rdd_partition[self.rdd_id].data:
                yield element


    def cache(self, rdd_partition = None):
        self.data = [self.func(element) for element in self.parent.get(rdd_partition)]
        self.is_cached = True

        # print self.data
        # print "Cache the result for MapPartition"
        #return self


class MapValuePartition(Partition):

    def __init__(self, rdd_id, parent, func):
        super(MapValuePartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self, rdd_partition = None):
        if self.rdd_id not in rdd_partition:
            if self.is_cached:
                for element in self.data:
                    yield element
            else:
                for key, value in self.parent.get(rdd_partition):
                    yield (key, self.func(value))
        else:
            for element in rdd_partition[self.rdd_id].data:
                yield element

    def cache(self, rdd_partition = None):
        self.data = [(key, self.func(value)) for key, value in self.parent.get(rdd_partition)]
        self.is_cached = True

        # print self.data
        # print "Cache the result for MapValuePartition"
        #return self


class FlatMapPartition(Partition):
    def __init__(self, rdd_id, parent, func):
        super(FlatMapPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self, rdd_partition = None):
        if self.rdd_id not in rdd_partition:
            if self.is_cached:
                for element in self.data:
                    yield element
            else:
                for element in self.parent.get(rdd_partition):
                    for i in self.func(element):
                        yield i
        else:
            for element in rdd_partition[self.rdd_id].data:
                yield element

    def cache(self, rdd_partition = None):
        self.data = []
        for element in self.parent.get(rdd_partition):
            for i in self.func(element):
                self.data.append(i)
        self.is_cached = True

        # print self.data
        # print "Cache the result for FlatMapPartition"
        #return self


class FilterPartition(Partition):
    
    def __init__(self,  rdd_id, parent, func):
        super(FilterPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self, rdd_partition = None):
        if self.rdd_id not in rdd_partition:
            if self.is_cached:
                for element in self.data:
                    yield element
            else:
                for element in self.parent.get(rdd_partition):
                    if self.func(element):
                        yield element
        else:
            for element in rdd_partition[self.rdd_id].data:
                yield element


    def cache(self, rdd_partition = None):
        self.data = [element for element in self.parent.get(rdd_partition) if self.func(element)]
        self.is_cached = True

        # print self.data
        # print "Cache the result for FilePartition"
        #return self


class GroupByKeyPartition(Partition):
    def __init__(self, rdd_id, parent):
        super(GroupByKeyPartition, self).__init__(rdd_id)
        self.parent = parent

    def get(self, rdd_partition = None):
        '''
        This function can be optimized by not creating list directly
        Should do the optimization later
        '''
        if self.rdd_id not in rdd_partition:
            if self.is_cached:
                for element in self.data:
                    yield element
            else:
                self.cache(rdd_partition)
                for element in self.data:
                    yield element
        else:
            for element in rdd_partition[self.rdd_id].data:
                yield element

    def set_partition_index(self, index):
        self.partition_index = index

    def cache(self, rdd_partition = None):
        parent_rdd = [element for element in self.parent.get(rdd_partition)]
        sorted_rdd = sorted(parent_rdd)
        self.data = [(key, [i[1] for i in group]) for key, group in groupby(sorted_rdd, lambda x: x[0])]
        self.is_cached = True

        # print self.data
        # print "Cache the result for GroupByKeyPartition"
        #return self


class ReduceByKeyPartition(Partition):
    def __init__(self, rdd_id, parent, func):
        super(ReduceByKeyPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self, rdd_partition= None):
        if self.rdd_id not in rdd_partition:
            if self.is_cached:
                for element in self.data:
                    yield element
            else:
                self.cache(rdd_partition)
                for element in self.data:
                    yield element
        else:
            for element in rdd_partition[self.rdd_id].data:
                yield element            

    def set_partition_index(self, index):
        self.partition_index = index

    def cache(self, rdd_partition= None):
        parent_rdd = [element for element in self.parent.get(rdd_partition)]
        sorted_rdd = sorted(parent_rdd)
        group_data = [(key, [i[1] for i in group]) for key, group in groupby(sorted_rdd, lambda x: x[0])]
        self.data = [(key, reduce(self.func, group)) for key, group in group_data]
        self.is_cached = True

        # print self.data
        # print "Cache the result for ReduceByKeyPartition"
        #return self


class JoinPartition(Partition):
    def __init__(self, rdd_id, parent_1, parent_2):
        super(JoinPartition, self).__init__(rdd_id)
        self.parent_1 = parent_1
        self.parent_2 = parent_2

    def get(self, rdd_partition= None):
        '''
        now we just compare each element with all the elements in the other lists
        we should optimize this function buy sorting the two list first then compare 
        one by one
        '''
        if self.rdd_id not in rdd_partition:
            if self.is_cached:
                for element in self.data:
                    yield element
            else:
                for i in self.parent_1.get(rdd_partition):
                    for j in self.parent_2.get(rdd_partition):
                        if i[0] == j[0]:
                            #print (i[0], (i[1], j[1]))
                            yield (i[0], (i[1], j[1]))
        else:
            for element in rdd_partition[self.rdd_id].data:
                yield element

    def set_partition_index(self, index):
        self.partition_index = index

    def cache(self, rdd_partition= None):
        self.data = []
        for i in self.parent_1.get(rdd_partition):
            for j in self.parent_2.get(rdd_partition):
                if i[0] == j[0]:
                    self.data.append((i[0], (i[1], j[1])))
        self.is_cached = True

        # print self.data
        # print "Cache the data for JoinPartition"
        #return self

#RePartition rdd_id = parent.id + 1 
class RePartition(Partition):
    def __init__(self, rdd_id, parent, worker_num):
        super(RePartition, self).__init__(rdd_id)
        self.parent = parent

        #default hash
        self.func = lambda x:(hash(x[0]) % worker_num)
        self.split_result = {}
        self.worker_conn = None
        self.driver = None
        self.partition_index = 0

    def is_repartition(self):
        return True

    def get(self, rdd_partition = None):
        '''
        Because zerorpc will convert tuples to list, we have to convert
        the element back to tuples that's why we do 
        yield (element[0], element[1])
        '''
        if self.rdd_id not in rdd_partition:
            if self.is_cached:
                for element in self.data:
                    yield element
            else:
                self.cache(rdd_partition)
                for element in self.data:
                    yield element
        else:
            for element in rdd_partition[self.rdd_id].data:
                yield (element[0], element[1])


    def cache(self, rdd_partition=None):
        for element in self.parent.get(rdd_partition):
            if self.func(element) in self.split_result.keys():
                self.split_result[self.func(element)].append(element)
            else:
                self.split_result[self.func(element)] = [element]

        #Initiallize self.data
        if self.partition_index in self.split_result:
            self.data = self.data + self.split_result[self.partition_index]
        # print "This is the local assigned data:"
        # print self.data

        # print "************* self.worker_conn in RePartition **********"
        # print self.worker_conn
        for index, conn in self.worker_conn.iteritems():
            if index in self.split_result:
                conn.collect_data(self.rdd_id, self.split_result[index])

        self.is_cached = True

        # print "Cached the data for the RePartition:"
        # print self.data


    def collect_data(self, split):
        self.data = self.data + split


if __name__ == "__main__":

    r = FilePartition(1, 'myfile', 1)
    f = FlatMapPartition(2, r, lambda s: s.split())
    m = MapPartition(3, f, lambda s:(s, 1))
    p = RePartition(4, m, 2)
    p.func = lambda x:(hash(x[0]) % 2)
    p.partition_index = 1
    p.cache()
    r = ReduceByKeyPartition(5, p, lambda x, y: x + y)
    r.cache()
    print r.data
