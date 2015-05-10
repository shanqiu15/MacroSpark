#!bin/bash
cd /Local/Users/hao/Desktop/MacroSpark
python worker.py 127.0.0.1:9001 &
python worker.py 127.0.0.1:9002 &
python worker.py 127.0.0.1:9003 &
