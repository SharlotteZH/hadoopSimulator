#!/usr/bin/env python

from optparse import OptionParser

import random
import sys

from simulator import Simulator
from node import Node
from job import Job
from workloadmanager import WorkloadManager

def getPriority(nReds):
	if nReds<3:
		return Job.Priority.VERY_HIGH
	elif nReds < 4:
		return Job.Priority.HIGH
	elif nReds < 5:
		return Job.Priority.NORMAL
	elif nReds < 6:
		return Job.Priority.LOW
	else:
		return Job.Priority.VERY_LOW

def getProbabilisticSJF(nReds, prob):
	if random.random() < prob:
		return getPriority(nReds)
	else:
		return Job.Priority.NORMAL

def parseSchedule(sched):
	tmp = []
	res = {}
	if len(sched) <= 0:
		return res
	tmp = sched.split(',')
	perc = 0.0
	for e in tmp:
		(k,v) = e.split(':')
		perc += float(k)
		res[perc] = float(v)
	return res

def getProbBySchedule(d, nReds):
	t = random.random()
	for i in sorted(d):
		if t <= i:
			return getProbabilisticSJF(nReds, d[i])

def unit_test():
	for i in range(0, 20):
		job = Job(nmaps=64, lmap=140, nreds=1, lred=15, submit=i*150)
		if random.random() < 0.5:
			job.priority = Job.Priority.VERY_HIGH
		jobId = simulator.addJob(job)

	for jobID in simulator.jobsQueue:
		myjob = simulator.jobs[jobID]
		print myjob.jobId, myjob.priority, myjob.submit

if __name__ == "__main__":
	# Parse options
	parser = OptionParser()
	parser.add_option('-l', "--log",                        dest="log",                      default=None,  help="specify the log file")

	parser.add_option('-n', "--nodes",                      dest="nodes",     type="int",    default=9,    help="specify the number of nodes")
	parser.add_option('-x', "--mapslot",                      dest="mapslot",     type="int",    default=4,    help="specify the number of map slots")
	parser.add_option('-y', "--redslot",                      dest="redslot",     type="int",    default=1,    help="specify the number of reduce slots")
	parser.add_option('-j', "--jobs",                       dest="jobs",      type="int",    default=20,    help="specify the number of jobs")
	parser.add_option('-g', "--gauss",                      dest="gauss",     type="float",  default=None,  help="specify the variance of the task length")

	parser.add_option('-r', "--real",action="store_true", dest="realistic", default=False, help="run a realistic simulation")
	parser.add_option('-m',"--manage",action="store_true",dest="manage",default=False,help="manage node disabled by default")

	parser.add_option('-s', "--sjf",                        dest="sjf",       type="float",  default=0.0,   help="specify the percentage of newly submitted job using SJF scheduling")
	parser.add_option('-w', "--weight",                     dest="weight",       type="string",  default="",   help="specify the detailed cheduling weight [X%:Y]")
	parser.add_option('-f', "--infile",                     dest="infile",    type="string", default="",    help="workload file")
      parser.add_option('-sp', "--schedulingPolicy",                     dest="schedulingPolicy",    type="int", default=2,    help="SJF=0, FIFO=1, FIFOPR = 2")


	(options, args) = parser.parse_args()
	#options.realistic
	options.og = None
	# Initialize simulator
	simulator = Simulator(logfile=options.log)
      simulator.schedulingPolicy = options.schedulingPolicy
	# Add servers
	for i in range(0, options.nodes):
		simulator.nodes['aws%03d' % i] = Node('aws%03d' % i) 
		simulator.nodes['aws%03d' % i].numMaps = options.mapslot
		simulator.nodes['aws%03d' % i].redMaps = options.redslot

	# Test
	#print 'run unit test'
	#unit_test()

	# Add jobs
	if len(options.infile) > 0: #wl
		simulator.nodeManagement = options.manage #disabled do not need to consider reboot
		manager = WorkloadManager(options.infile) #line by line 
		weights = {}
		weights = parseSchedule(options.weight) #advanced e.g. 25% short job first 75% FIFO
		for job in manager.getJobs():#based on arrival time
			if len(weights)>0:
				job.priority=getProbBySchedule(weights, job.nreds)
			else:
				job.priority=getProbabilisticSJF(job.nreds, options.sjf)
			simulator.addJob(job) #queue
	else:
		# Submit jobs
		for i in range(0, options.jobs):
			# Create the job
			job = Job(nmaps=64, lmap=140, nreds=1, lred=15, submit=0)
			job.gauss = options.gauss # +/-%
			# Probabilistic shortest job first policy
			job.priority = getProbabilisticSJF(job.nreds, options.sjf)
			jobId = simulator.addJob(job)

	# Start running simulator
	simulator.run()

	# Summary
	print 'Nodes:   %d'  %      len(simulator.nodes)
	print 'Perf:    %.1fs %d jobs' % (simulator.getPerformance(), len(simulator.jobs))

