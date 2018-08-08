import sys
import random

from job import Job
from simulator import Simulator

'''
Read workload from a file.
'''
class WorkloadManager:
        def __init__(self, filename=None):
		self.jobQueue = []
		if filename != None:
			self.read(filename)
		#self.jobIdQueue = []

	def read(self, inFile):
		#lineno=0
		with open(inFile, "r") as f:
			for line in f:
				line = line.replace('\n', '')
				line = line.strip()
				if not line.startswith('#') and len(line) > 0:
					splits = line.split()
					nmaps0 =      int(splits[0])
					lmap0 =       int(splits[1])
					nreds0 =      int(splits[2])
					lred0 =       int(splits[3])
					submit0 =     int(splits[4])
					# Create job
					job = Job(nmaps=nmaps0, lmap=lmap0, nreds=nreds0, lred=lred0, submit=submit0)
					self.jobQueue.append(job)
					#lineno+=1
		#return lineno
		return self.jobQueue

	def getJobs(self):
		return self.jobQueue


if __name__ == '__main__':
	workload = WorkloadManager(sys.argv[1])
