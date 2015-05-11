__author__ = 'mingluma'

import os


class Splitter(object):
    def __init__(self, path, worker_num):
        self.path = path
        self.worker_num = worker_num

    def split(self):
        """
        Split files into splits, each split has multiple chunks.
        :return: An array of split information, i.e., (file, start, end)
        """
        overall_chunks = []
        for filename in self.get_all_files():
            file_chunks = self.split_single_file(filename)
            overall_chunks.extend(file_chunks)
        return overall_chunks

    def get_all_files(self):
        if os.path.isdir(self.path):
            return os.listdir(self.path)
        else:
            return [self.path]

    def split_single_file(self, filename):
        """
        Split a single file into parts in chunk size
        :return: An array of split position of the chunks, i.e., (file, start, end).
        """
        file_size = os.path.getsize(filename)
        chunk_size = file_size / self.worker_num
        file_handler = open(filename, "r")
        chunks = []
        pos = 0
        while pos < file_size:
            next_pos = min(pos + chunk_size, file_size)
            if pos == 0:
                chunks.append((filename, pos, self.find_next_newline(file_handler, next_pos)))
            else:
                chunks.append((filename, self.find_next_newline(file_handler, pos), self.find_next_newline(file_handler, next_pos)))
            pos = next_pos
        file_handler.close()
        return chunks

    def find_next_newline(self, file_handler, pos):
        file_handler.seek(pos)
        line = file_handler.readline()
        return pos + len(line)

if __name__ == "__main__":
    splitter = Splitter("input", 4)
    print splitter.split()
