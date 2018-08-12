#!/usr/bin/env python

from commons import isRealistic

import random
if not isRealistic():
	random.seed(0)

class ID:
	@staticmethod
	def getId(id):
		return int(id.split('_')[-1])

"""
Represents a Hadoop attempt.
"""
class Attempt:
	def __init__(self, attemptId=None, task=None, seconds=10):
		self.task = task
		self.attemptId = attemptId
		self.nodeId = None
		self.start = None
		self.finish = None
		self.status = Job.Status.QUEUED
		self.seconds = seconds # Remaining seconds

	def progress(self, p):
		self.seconds -= p

	def isCompleted(self):
		return self.seconds <= 0

	def getJobId(self):
		return '_'.join(self.attemptId.split('_')[0:3]).replace('attempt_', 'job_')

	def getTaskId(self):
		return self.attemptId[:self.attemptId.rfind('_')].replace('attempt_', 'task_')

	def getId(self):
		return int(self.attemptId.split('_')[4])

	def isMap(self):
		return self.attemptId.rfind('_m_') >= 0

	def isRed(self):
		return self.attemptId.rfind('_r_') >= 0

	def getTask(self):
		return self.task

	def getJob(self):
		return self.getTask().getJob()

	def __str__(self):
		return self.attemptId

"""
Represents a Hadoop task.
"""
class Task:
	def __init__(self, taskId=None, job=None, length=None, gauss=None):
		self.job = job
		self.taskId = taskId
		self.length = length
		self.gauss = gauss # Task length distribution in %
		self.attempts = {}
		self.nattempts = 0
		self.status = Job.Status.QUEUED # Status: QUEUED -> RUNNING -> SUCCEEDED | DROPPED

	def isQueued(self):
		if len(self.attempts) == 0:
			return True
		else:
			for attempt in self.attempts.values():
				if attempt.status == Job.Status.QUEUED:
					return True
		return False

	def isMap(self):
		return self.taskId.rfind('_m_') >= 0

	def isRed(self):
		return self.taskId.rfind('_r_') >= 0

	def getAttempt(self):
		if len(self.attempts) == 0:
			self.nattempts += 1
			attemptId = (self.taskId+'_%d' % self.nattempts).replace('task_', 'attempt_')
			seconds = self.length
			if self.gauss != None: #random.gauss(mu, sigma) mean  standard deviation
				seconds = int(random.gauss(seconds, self.gauss/100.0*seconds))
				# Minimum task length
				if seconds < 3:
					seconds = 3
			attempt = Attempt(attemptId=attemptId, task=self, seconds=seconds)
			self.attempts[attempt.attemptId] = attempt
			return attempt
		else:
			for attempt in self.attempts.values():
				if attempt.status == Job.Status.QUEUED:
					return attempt
		return None

	def getJob(self):
		return self.job

"""
Represents a Hadoop job.
"""
class Job:
	# Job priorities
	class Priority:
		VERY_HIGH = 5
		HIGH      = 4
		NORMAL    = 3
		LOW       = 2
		VERY_LOW  = 1

	# Job status
	#  QUEUED -> RUNNING -> SUCCEEDED
	class Status:
		QUEUED    = 0
		RUNNING   = 1
		SUCCEEDED = 2
		toString = {QUEUED:'QUEUED', RUNNING:'RUNNING', SUCCEEDED:'SUCCEEDED'}

	def __init__(self, jobId=None, nmaps=16, lmap=60, nreds=1, lred=30, submit=0):
		self.jobId = jobId
		self.nmaps = nmaps
		self.nreds = nreds
		self.lmap = lmap
		self.lred = lred
		self.gauss = 20
		self.submit = submit # Submission time
		self.finish = None   # Finish time

		self.priority = Job.Priority.NORMAL

		# Set queue execution state
		self.reset()

	'''
	Reset the job running
	'''
	def reset(self):
		# Mark it as queued
		self.status = Job.Status.QUEUED
		# Reset Tasks
		self.cmaps = 0
		self.creds = 0
		self.maps = {}
		self.reds = {}

	def initTasks(self):
		# Maps
		# Initialize tasks
		for nmap in range(0, self.nmaps):
			taskId = '%s_m_%06d' % (self.jobId.replace('job_', 'task_'), nmap+1)
			self.maps[taskId] = Task(taskId, self, self.lmap)
			self.maps[taskId].gauss = self.gauss

		# Reduces
		# Initialize tasks
		for nred in range(0, self.nreds):
			taskId = '%s_r_%06d' % (self.jobId.replace('job_', 'task_'), nred+1)
			self.reds[taskId] = Task(taskId, self, self.lred)
			self.reds[taskId].gauss = self.gauss

	def getMapTask(self):
		for mapTask in self.maps.values():
			if mapTask.isQueued():
				return mapTask.getAttempt()
		return None

	def getRedTask(self):
		# Wait for the maps to finish. TODO slow start
		if self.cmaps >= len(self.maps):
			for redTask in self.reds.values():
				if redTask.isQueued():
					return redTask.getAttempt()
		return None

	def mapQueued(self):
		ret = 0
		for mapTask in self.maps.values():
			if mapTask.isQueued():
				ret += 1
		return ret

	def redQueued(self):
		ret = 0
		if self.isMapCompleted():
			for redTask in self.reds.values():
				if redTask.isQueued():
					ret += 1
		return ret

	def getStart(self):
		start = None
		for task in self.maps.values() + self.reds.values():
			for attempt in task.attempts.values():
				if start == None or start > attempt.start:
					start = attempt.start
		return start

	def getFinish(self):
		finish = None
		for task in self.maps.values() + self.reds.values():
			for attempt in task.attempts.values():
				if finish == None or finish < attempt.finish:
					finish = attempt.finish
		return finish

	# Check if all the maps are completed
	def isMapCompleted(self):
		return self.cmaps >= len(self.maps)

	# Check if the job is running
	def isRunning(self):
		return self.cmaps < len(self.maps) or self.creds < len(self.reds)

	# Get the list of nodes that have run this node
	def getNodes(self):
		ret = []
		for task in self.maps.values() + self.reds.values():
			for attempt in task.attempts.values():
				if attempt.nodeId != None and attempt.nodeId not in ret:
					ret.append(attempt.nodeId)
		return ret

	# Complete an attempt
	def completeAttempt(self, attempt):
		ret = []
		# Map
		if attempt.isMap():
			for mapTask in self.maps.values():
				if mapTask.taskId == attempt.getTaskId():
					mapTask.status = Job.Status.SUCCEEDED
					self.cmaps += 1
		# Reduce
		else:
			for redTask in self.reds.values():
				if redTask.taskId == attempt.getTaskId():
					redTask.status = Job.Status.SUCCEEDED
					self.creds += 1
			if self.creds >= len(self.reds):
				ret.append(self)
		return ret

	# Add an attempt to the job
	def addAttempt(self, attempt):
		taskId = attempt.getTaskId()
		if attempt.isMap():
			if taskId not in self.maps:
				self.maps[taskId] = Task(taskId)
			self.maps[taskId].attempts[attempt.attemptId] = attempt
		if attempt.isRed():
			if taskId not in self.reds:
				self.reds[taskId] = Task(taskId)
			self.reds[taskId].attempts[attempt.attemptId] = attempt

