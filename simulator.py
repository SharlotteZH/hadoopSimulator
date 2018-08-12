#!/usr/bin/env python

from commons import isRealistic

import math
import random
if not isRealistic():
	random.seed(0)

from operator import attrgetter

from node import Node
from job import Job
from schedulerpolicy import SchedulerPolicy
from history import History
from history import HistoryViewer
import sys
from datetime import datetime



class Simulator(SchedulerPolicy):
	def __init__(self, logfile='history.log'):
		# Initialize the scheduler
		SchedulerPolicy.__init__(self) # super()
           #default: self.schedType = SchedulerPolicy.Type.FIFOPR
		self.t = 0
		# Nodes
		self.nodes = {}
		# History
		self.logfile = logfile
		self.history = History(filename=self.logfile)
		# Job submission
		self.lastJobId = 1
		# Simulation
		self.maxTime = None
		# Id for jobs
		if sys.platform == 'win32':
			self.trackerId = datetime.now().strftime('%Y%m%d%H%M')
		else:
			self.trackerId = datetime.now().strftime('%4Y%2m%2d%2H%2M')
		# Specify if the nodes are sent to sleep when there's no load
		self.nodeManagement = True

		# Outputs
		self.energy = None

		# Step length
		self.STEP = 1

	# Submit a job to run
	def addJob(self, job):
		# Assign automatic job id
		if job.jobId == None:
			while 'job_%s_%04d' % (self.trackerId, self.lastJobId) in self.jobs:
				self.lastJobId += 1
			job.jobId = 'job_%s_%04d' % (self.trackerId, self.lastJobId)
		# Initialize tasks
		job.initTasks()

		# Save the information
		self.jobs[job.jobId] = job
		self.jobsQueue.append(job.jobId)

		# Sort the queue according to submission order/FIFOPR
		self.jobsQueue = sorted(self.jobsQueue, cmp=self.schedulingPolicy)

		return job.jobId

	# Check if there is any idle node for reduces
	def getIdleNodeMap(self):
		for nodeId in sorted(self.nodes):
			node = self.nodes[nodeId]
			if node.status == 'ON' and len(node.maps) < node.numMaps:
				return node
		return None

	def getIdleNodesMap(self):
		ret = []
		for nodeId in sorted(self.nodes):
			node = self.nodes[nodeId]
			if node.status == 'ON' and len(node.maps) < node.numMaps:
				ret.append(node)
		return ret

	# Check if there is any idle node for reduces
	def getIdleNodeRed(self):
		for nodeId in sorted(self.nodes):
			node = self.nodes[nodeId]
			if node.status == 'ON' and len(node.reds) < node.numReds:
				return node
		return None

	def getIdleNodesRed(self):
		ret = []
		for nodeId in sorted(self.nodes):
			node = self.nodes[nodeId]
			if node.status == 'ON' and len(node.reds) < node.numReds:
				ret.append(node)
		return ret

	def getWakingNodes(self):
		ret = 0
		for nodeId in self.nodes:
			node = self.nodes[nodeId]
			if node.status.startswith('WAKING-'):
				ret += 1
		return ret

	# Get a queued map
	def getMapTask(self):
		for jobId in self.jobsQueue:
			job = self.jobs[jobId]
			if self.t >= job.submit:
				mapTask = job.getMapTask()
				if mapTask != None:
					return mapTask
		return None

	# Get a queued reduce
	def getRedTask(self):
		for jobId in self.jobsQueue:
			job = self.jobs[jobId]
			if self.t >= job.submit:
				redTask = job.getRedTask()
				if redTask != None:
					return redTask
		return None

	# Check if there is a map queued
	def mapQueued(self):
		ret = 0
		for jobId in self.jobsQueue:
			job = self.jobs[jobId]
			if self.t >= job.submit:
				ret += job.mapQueued()
		return ret

	# Check if the node is required: running job or providing data for a job
	def isNodeRequired(self, nodeId):
		node = self.nodes[nodeId]
		# Check if the node is in the covering subset (data) or is running
		if node.covering or node.isRunning():
			return True
		# Check if it has executed tasks from active tasks
		for jobId in self.jobsQueue:
			job = self.jobs[jobId]
			if job.isRunning() and nodeId in job.getNodes():
				return True
		return False

	# Check if there is a reduce queued
	def redQueued(self):
		ret = 0
		for jobId in self.jobsQueue:
			job = self.jobs[jobId]
			if self.t >= job.submit:
				ret += job.redQueued()
		return ret

	def getNodesUtilization(self):
		utilizations = []
		for nodeId in self.nodes:
			node = self.nodes[nodeId]
			if node.status == 'ON':
				utilization = 1.0*len(node.maps)/node.numMaps
				utilizations.append(utilization)
		return sum(utilizations)/len(utilizations) if len(utilizations)>0 else 1.0

	def getNodesRunning(self):
		ret = 0
		for nodeId in self.nodes:
			node = self.nodes[nodeId]
			if node.status == 'ON':
				ret += 1
		return ret

	# Energy in Wh
	def getEnergy(self):
		# J = Ws -> Wh
		return self.energy/3600.0

	# Average time to run per job in seconds
	def getPerformance(self):
		ret = None
		if len(self.jobs) > 0:
			ret = 0.0
			for jobId in self.jobs:
				job = self.jobs[jobId]
				ret += job.getFinish()
			ret = ret / len(self.jobs)
		return ret


	def isTimeLimit(self):
		return not (self.maxTime==None or self.t < self.maxTime)

	# Run simulation
	def run(self):
		# Log initial node status
		for nodeId in self.nodes:
			node = self.nodes[nodeId]
			self.history.logNodeStatus(self.t, node)

		# Iterate every X seconds
		while len(self.jobsQueue) > 0 and not self.isTimeLimit():
			# Run running tasks
			# =====================================================
			completedAttempts = []
			for node in self.nodes.values():
				completedAttempts += node.progress(self.STEP) # progress 1 second at a time

			# Mark completed maps
			completedJobs = []
			for attempt in completedAttempts:
				attempt.finish = self.t
				# Check if we finish the jobs
				completedJobs += attempt.getJob().completeAttempt(attempt)
				# Log
				self.history.logAttempt(attempt)

			for job in completedJobs:
				job.finish = self.t
				job.status = Job.Status.SUCCEEDED
				# Update queues
				self.jobsQueue.remove(job.jobId)
				self.jobsDone.append(job.jobId)
				# Log
				self.history.logJob(job)

			# Check which nodes are available to run tasks
			# =====================================================
			# Maps
			while self.mapQueued()>0 and self.getIdleNodeMap() != None:
				# Get a map that needs to be executed and assign it to a node
				idleNode = self.getIdleNodeMap()
				mapAttempt = self.getMapTask()
				mapAttempt.start = self.t
				# Start running in a node
				idleNode.assignMap(mapAttempt)
			# Reduces
			while self.redQueued()>0 and self.getIdleNodeRed() != None:
				# Get a map that needs to be executed and assign it to a node
				idleNode = self.getIdleNodeRed()
				redAttempt = self.getRedTask()
				redAttempt.start = self.t
				idleNode.assignRed(redAttempt)


			# Progress to next period
			self.t += self.STEP

		# Log final output
		if self.logfile != None:
			self.history.close()
			viewer = HistoryViewer(self.history.getFilename())
			viewer.generate()
