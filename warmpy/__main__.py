import sys
from .core import supervise, work

if sys.argv[1] == '--worker': work(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
else: supervise(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
