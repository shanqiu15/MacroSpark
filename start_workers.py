__author__ = 'hao'

#!/usr/bin/env python
'''
python start_workers.py hao@localhost start_workers.sh
'''

import os
import os.path
import subprocess
import sys
import gevent
from gevent.pool import Group

class Remote(object):

    def __init__(self, host, filename):
        self.host = host
        self.filename = filename
        self.curdir = os.getcwd() #get the current working directory
        self.remote_file = os.path.join(self.curdir, self.filename)

    def run(self):
        subprocess.call(['ssh', self.host, "bash", self.remote_file])


if __name__ == '__main__':
    remote = Remote(sys.argv[1], sys.argv[2])
    remote.run()
