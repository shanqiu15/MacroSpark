from itertools import groupby

class Partition(object):

    def __init__(self, rdd_id, partition_index = None):
        self.rdd_id = rdd_id
        self.partition_index = partition_index
        self.data = []
        self.is_cached = False

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

    def get_data(self):
        if self.is_cached:
            return self.data
        else:
            self.cache()
            return self.data


class FilePartition(Partition):

    def __init__(self, rdd_id, filename, worker_num):
        super(FilePartition, self).__init__(rdd_id)
        self.filename = filename
        self.partition_num = worker_num

    def get(self):
        print "This is the get in TextFile"
        if not self.data:
            f = open(self.filename)
            self.data = f.readlines()
            f.close()
    
        for line in self.data:
            yield line

    def cache(self):
        f = open(self.filename)
        self.data = f.readlines()
        f.close()
        self.is_cached = True
        print self.data
        print "Cached the result for FilePartition" 


class MapPartition(Partition):

    def __init__(self, rdd_id, parent, func):
        super(MapPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self):
        print "This is the caculation in mapper"
        if self.is_cached:
            for element in self.data:
                yield element
        else:
            for element in self.parent.get():
                yield self.func(element)

    def cache(self):
        self.data = [element for element in self.parent.get()]
        self.is_cached = True
        print self.data
        print "Cache the result for MapPartition"
        #return self


class MapValuePartition(Partition):

    def __init__(self, rdd_id, parent, func):
        super(MapValuePartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self):
        print "This is the caculation in MapValuePartition"
        if self.is_cached:
            for element in self.data:
                yield element
        else:
            for key, value in self.parent.get():
                yield (key, self.func(value))

    def cache(self):
        self.data = [(key, self.func(value)) for key, value in self.parent.get()]
        self.is_cached = True
        print self.data
        print "Cache the result for MapValuePartition"
        #return self


class FlatMapPartition(Partition):
    def __init__(self, rdd_id, parent, func):
        super(FlatMapPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self):
        print "This is the caculation in mapper"
        if self.is_cached:
            for element in self.data:
                yield element
        else:
            for element in self.parent.get():
                for i in self.func(element):
                    yield i

    def cache(self):
        self.data = []
        for element in self.parent.get():
            for i in self.func(element):
                self.data.append[i]
        self.is_cached = True
        print self.data
        print "Cache the result for FlatMapPartition"
        #return self


class FilterPartition(Partition):
    
    def __init__(self,  rdd_id, parent, func):
        super(FilterPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self):
        print "This is the caculation in filter"
        if self.is_cached:
            for element in self.data:
                yield element
        else:
            for element in self.parent.get():
                if self.func(element):
                    yield element

    def cache(self):
        self.data = [element for element in self.parent.get() if self.func(element)]
        self.is_cached = True
        print self.data
        print "Cache the result for FilePartition"
        #return self


class GroupByKeyPartition(Partition):
    def __init__(self,  rdd_id, parent):
        super(GroupByKeyPartition, self).__init__(rdd_id)
        self.parent = parent

    def get(self):
        '''
        This function can be optimized by not creating list directly
        Should do the optimization later
        '''
        if self.is_cached:
            for element in self.data:
                yield element
        else:
            parent_rdd = [element for element in self.parent.get()]
            sorted_rdd = sorted(parent_rdd)
            group_rdd = [(key, [i[1] for i in group]) for key, group in groupby(sorted_rdd, lambda x: x[0])]
            for element in group_rdd:
                yield element

    def cache(self):
        parent_rdd = [element for element in self.parent.get()]
        sorted_rdd = sorted(parent_rdd)
        self.data = [(key, [i[1] for i in group]) for key, group in groupby(sorted_rdd, lambda x: x[0])]
        self.is_cached = True
        print self.data
        print "Cache the result for GroupByKeyPartition"
        #return self


class ReduceByKeyPartition(Partition):
    def __init__(self, rdd_id, parent, func):
        super(ReduceByKeyPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self):
        if self.is_cached:
            for element in self.data:
                yield element
        else:
            grouper =  GroupByKeyPartition(-1 , parent)
            for key, group in grouper.get():
                yield (key, reduce(self.func, group))

    def cache(self):
        grouper =  GroupByKeyPartition(-1 , parent) 
        self.data = [(key, reduce(self.func, group)) for key, group in grouper.get()]
        self.is_cached = True
        print self.data
        print "Cache the result for ReduceByKeyPartition"
        #return self


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
        if self.is_cached:
            for element in self.data:
                yield element
        else:
            for i in self.parent_1.get():
                for j in self.parent_2.get():
                    if i[0] == j[0]:
                        print (i[0], (i[1], j[1]))
                        yield (i[0], (i[1], j[1]))

    def cache(self):
        self.data = []
        for i in self.parent_1.get():
            for j in self.parent_2.get():
                if i[0] == j[0]:
                    self.data.append((i[0], (i[1], j[1])))

        self.is_cached = True
        print self.data
        print "Cache the data for JoinPartition"
        #return self



#RePartition rdd_id = parent.id + 1 
class RePartition(Partition):
    def __init__(self, rdd_id, parent,func = None):
        super(RePartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func
        self.split_result = {}
        self.worker_conn = None
        self.driver = None


    def setup_connections(worker_conn, driver_conn):        
        self.driver_conn = driver_conn
        self.worker_conn = worker_conn

    def get(self):
        '''
        should implement repartition here
        '''
        if self.is_cached:
            for element in self.data:
                yield element
        else:
            raise Exception("no data available")

    def cache(self):
        for element in self.parent.get():
            if self.func(element) in self.split_result.keys():
                self.split_result[self.func(element)].append(element)
            else:
                self.split_result[self.func(element)] = [element]

        #Initiallize self.data
        self.data = self.data + self.split_result[self.partition_index]

        for index, conn in self.worker_conn:
            conn.collect_data(self.split_result[index])

        self.is_cached = True
        self.driver_conn.repartition_acc()
        print self.data
        print "Cache the data for the RePartition"
        return self.partition_index

    def collect_data(self, split):
        self.data = self.data + split




if __name__ == "__main__":

    r = FilePartition(1, 1, 'myfile')
    m = MapPartition(2, 1, r, lambda s: s.split())
    f = FilterPartition(3, 1, m, lambda a: int(a[1]) > 2)

    z = FilePartition(4 , 1, 'myfile')
    q = MapPartition(5, 1, r, lambda s: s.split())
    j = JoinPartition(6, 1, m, q)
    print j.collect()