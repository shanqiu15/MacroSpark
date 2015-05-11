__author__ = 'hao'

#!/usr/bin/env python2.7
'''
python start_workers.py hao@localhost start_workers.sh
'''

import inspect
import os
import os.path
import subprocess
import sys

class Remote(object):
    def __init__(self, worker_addr):
        self.host = worker_addr.split(":")[0]
        self.port = worker_addr.split(":")[1]
        self.root_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

    def start(self):
        subprocess.Popen(['ssh',
		          self.host,
                          "export PATH=$HOME/bin:$PATH; python " + self.root_dir + "/worker.py 127.0.0.1:" + self.port])

    def stop(self):
        subprocess.Popen([
            'ssh',
            self.host,
            "ps aux |grep 127.0.0.1:" + self.port + "| awk '{print $2}' |xargs kill -9"
        ])


if __name__ == '__main__':
    remotes = [
        Remote("bass16.cs.usfca.edu:23000"),
        Remote("bass16.cs.usfca.edu:23001"),
        Remote("bass16.cs.usfca.edu:23002"),
        #Remote("127.0.0.1:9001"),
        #Remote("127.0.0.1:9002"),
        #Remote("127.0.0.1:9003"),
    ]

    for r in remotes:
        if sys.argv[1] == "start":
            r.start()
        elif sys.argv[1] == "stop":
            r.stop()
