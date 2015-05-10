from itertools import groupby
import os

class Partition(object):

    def __init__(self, rdd_id, partition_index = None):
        self.rdd_id = rdd_id
        self.partition_index = partition_index
        self.data = []
        self.is_cached = False

    def set_partition_index(self, index):
        self.partition_index = index

    # def set_start_stage(self,rdd_partition):
    #     '''
    #     This method should be overwrite for join and FilePartition
    #     Because join has two parent and FilePartition has no parent
    #     '''
    #     if self.parent is None:
    #         return
    #     else:
    #         while True:
    #             parent = self.parent
    #             if parent.rdd_id in rdd_partition.keys():
    #                 self.parent = rdd_partition[parent.rdd_id]
    #             :















    def collect(self):
        elements = []
        for element in self.get():
            elements.append(element)
        return elements

    def show(self):
        for elem in self.get():
            print elem

    def is_repartition(self):
        return False

    def get_data(self):
        if self.is_cached:
            return self.data
        else:
            self.cache()
            return self.data

    def setup_connections(self, worker_conn, driver_conn, index):        
        self.driver_conn = driver_conn
        self.worker_conn = worker_conn
        self.partition_index = index


class FilePartition(Partition):

    def __init__(self, rdd_id, input_path, worker_num):
        super(FilePartition, self).__init__(rdd_id)
        self.inputPath = input_path
        self.partition_num = worker_num
        self.parent = None
        self.split_result = []

    def get(self, rdd_partition = None):
        if not self.data:
            split_result = self.split()
            file_reader = open(split_result[0], "r")
            file_reader.seek(split_result[1])
            self.data = file_reader.read(split_result[2] - split_result[1])
            file_reader.close()
    
        for line in self.data:
            yield line

    def cache(self, rdd_partition = None):
        split_result = self.split()
        file_reader = open(split_result[0], "r")
        file_reader.seek(split_result[1])
        self.data = file_reader.read(split_result[2] - split_result[1])
        file_reader.close()
        self.is_cached = True
        print self.data
        print "Cached the result for FilePartition"

    def split(self):
        """
        Split files into splits, each split has multiple chunks.
        :return: An array of split information, i.e., (file, start, end)
        """
        overall_chunks = []
        for single_file in self.get_all_files():
            file_chunks = self.split_single_file(single_file)
            overall_chunks.extend(file_chunks)
        return overall_chunks

    def get_all_files(self):
        if os.path.isdir(self.input_path):
            return os.listdir(self.input_path)
        else:
            return [self.input_path]

    def split_single_file(self, single_file):
        """
        Split a single file into parts in chunk size
        :return: An array of split position of the chunks, i.e., (file, start, end).
        """
        file_size = os.path.getsize(file)
        chunk_size = file_size / self.partition_num
        file_handler = open(single_file, "r")
        chunks = []
        pos = 0
        while pos < file_size:
            next_pos = min(pos + chunk_size, file_size)
            if pos == 0:
                chunks.append((file, pos, self.find_next_newline(file_handler, next_pos)))
            else:
                chunks.append((file, self.find_next_newline(file_handler, pos),
                               self.find_next_newline(file_handler, next_pos)))
            pos = next_pos
        file_handler.close()
        return chunks

    def find_next_newline(self, file_handler, pos):
        file_handler.seek(pos)
        line = file_handler.readline()
        return pos + len(line)


class MapPartition(Partition):

    def __init__(self, rdd_id, parent, func):
        super(MapPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self, rdd_partition = None):
        print "This is the caculation in mapper"
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
        print self.data
        print "Cache the result for MapPartition"
        #return self


class MapValuePartition(Partition):

    def __init__(self, rdd_id, parent, func):
        super(MapValuePartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self, rdd_partition = None):
        print "This is the caculation in MapValuePartition"
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
        print self.data
        print "Cache the result for MapValuePartition"
        #return self


class FlatMapPartition(Partition):
    def __init__(self, rdd_id, parent, func):
        super(FlatMapPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self, rdd_partition = None):
        print "This is the caculation in flat mapper"
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
        print self.data
        print "Cache the result for FlatMapPartition"
        #return self


class FilterPartition(Partition):
    
    def __init__(self,  rdd_id, parent, func):
        super(FilterPartition, self).__init__(rdd_id)
        self.parent = parent
        self.func = func

    def get(self, rdd_partition = None):
        print "This is the caculation in filter"
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
        print self.data
        print "Cache the result for FilePartition"
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

    def cache(self, rdd_partition = None):
        parent_rdd = [element for element in self.parent.get(rdd_partition)]
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


    def cache(self, rdd_partition= None):
        parent_rdd = [element for element in self.parent.get(rdd_partition)]
        sorted_rdd = sorted(parent_rdd)
        group_data = [(key, [i[1] for i in group]) for key, group in groupby(sorted_rdd, lambda x: x[0])]
        self.data = [(key, reduce(self.func, group)) for key, group in group_data]
        self.is_cached = True
        print self.data
        print "Cache the result for ReduceByKeyPartition"
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

    def cache(self, rdd_partition= None):
        self.data = []
        for i in self.parent_1.get(rdd_partition):
            for j in self.parent_2.get(rdd_partition):
                if i[0] == j[0]:
                    self.data.append((i[0], (i[1], j[1])))
        self.is_cached = True
        print self.data
        print "Cache the data for JoinPartition"
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
        print "Call the get in RePartition"
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
        self.data = self.data + self.split_result[self.partition_index]
        print "This is the local assigned data:"
        print self.data

        print "************* self.worker_conn in RePartition **********"
        print self.worker_conn
        for index, conn in self.worker_conn.iteritems():
            conn.collect_data(self.rdd_id, self.split_result[index])

        self.is_cached = True
        print "Cached the data for the RePartition:"
        print self.data


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

    # r = FilePartition(1, 1, 'myfile')
    # m = MapPartition(2, 1, r, lambda s: s.split())
    # f = FilterPartition(3, 1, m, lambda a: int(a[1]) > 2)

    # z = FilePartition(4 , 1, 'myfile')
    # q = MapPartition(5, 1, r, lambda s: s.split())
    # j = JoinPartition(6, 1, m, q)
    # print j.collect()