"""
This script is used to run multiple instances of get_info.py across all cores
of our machine.
"""

import subprocess as sp
from multiprocessing import Pool
import random
import sys


N = int(sys.argv[1])  # User inputs the total number of records in our database
chunks = 32
delta = (N / chunks) + 1
start_end_pairs = [(i * delta, (i+1) * delta) for i in xrange(chunks)]


def run_task(args):
    start, end = args
    sp.call('python get_info.py %s %s &' % (start, end), shell=True)


if __name__ == '__main__':
    pool = Pool(8)
    pool.map(run_task, start_end_pairs)
