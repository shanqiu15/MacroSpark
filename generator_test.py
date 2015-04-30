
class Generator_test(object):
    def __init__(self):
        self.new_rdd = "initial value"
        self.stage_index = 1

    def get_newRDD(self,stage_index, new_rdd):
        repartition_generator = self.do_repartition()
        repartition_generator.send(None)
        #######
        #place to do the repartition
        #new_rdd is the new generated rdd_partition
        #######
        repartition_generator.send(new_rdd)



    def do_repartition(self):
        '''
        The first yield will yield the current stage_index
        The second yield will yield the next stage_index
        '''
        #self.new_rdd = yield self.stage_index
        self.new_rdd = yield
        print self.stage_index, self.new_rdd
        self.stage_index = self.stage_index + 1
        #yield self.stage_index
        yield


if __name__ == "__main__":
    g =  Generator_test()
    g.get_newRDD(1, "This is the new value")
    g.get_newRDD(1, "This is the other new value")
    print g.new_rdd
