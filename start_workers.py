__author__ = 'hao'

#!/usr/bin/env python
'''
python start_workers.py hao@localhost start_workers.sh
'''

import inspect
import os
import os.path
import subprocess
import sys

MACRO_SPARK_DIR = "/Users/mingluma/2015Spring/OS2015s/macroSpark/MacroSpark"
#MACRO_SPARK_DIR = "/Local/Users/hao/Desktop/MacroSpark/worker.py"

class Remote(object):
    def __init__(self, worker_addr):
        self.host = worker_addr.split(":")[0]
        self.port = worker_addr.split(":")[1]
        self.root_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

    def start(self):
        subprocess.Popen(['ssh',
                              self.host,
                              "python",
                              MACRO_SPARK_DIR + "/worker.py",
                              "127.0.0.1:" + self.port])

    def stop(self):
        subprocess.Popen([
            'ssh',
            self.host,
            "ps aux |grep 127.0.0.1:" + self.port + "| awk '{print \"kill -9 \" $2}' |bash"
        ])


if __name__ == '__main__':
    remotes = [
        Remote("127.0.0.1:9001"),
        Remote("127.0.0.1:9002"),
        Remote("127.0.0.1:9003"),
    ]

    for r in remotes:
        if sys.argv[1] == "start":
            r.start()
        elif sys.argv[1] == "stop":
            r.stop()
