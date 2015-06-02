__author__ = 'hao'

clean_file =  open("page_rank.txt", 'w')
file = open("pagerank.txt")
for line in file.readlines():
    l = line.split()
    if len(l) > 1:
        for i in range(len(l) - 1):
            clean_file.write(l[0] + ' ' + l[i + 1] + '\n')
    else:
        clean_file.write(l[0]+'\n')
