#!/usr/bin/env python

from job import Job

"""
Represents
"""
class Node:
	def __init__(self, nodeId=None):
		self.nodeId = nodeId
		self.status = 'ON' # ON -> SLEEPING-X -> SLEEP -> WAKING-X -> ON
		self.maps = [] # Attempts
		self.reds = [] # Attempts

		# Slots
		self.numMaps = 3
		self.numReds = 1

	'''
	Progress the execution of the node for a cycle.
	It returns the attempts that has finished.
	'''
	def progress(self, p):
		ret = []

		# Progress the execution of the tasks in this node
		# Maps
		for mapAttempt in list(self.maps):
			mapAttempt.progress(1)
			# Check if the map is completed
			if mapAttempt.isCompleted():
				self.maps.remove(mapAttempt)
				ret.append(mapAttempt)
		# Reduces
		for redAttempt in list(self.reds):
			# Check if the maps of this node are completed
			if redAttempt.getJob().isMapCompleted():
				redAttempt.progress(1)
			# Check if the reduce is completed
			if redAttempt.isCompleted():
				self.reds.remove(redAttempt)
				ret.append(redAttempt)
		return ret

	# Start running a map attempt
	def assignMap(self, attempt):
		self.maps.append(attempt)
		attempt.status = 'RUNNING'
		attempt.nodeId = self.nodeId

	# Start running a reduce attempt
	def assignRed(self, attempt):
		self.reds.append(attempt)
		attempt.status = 'RUNNING'
		attempt.nodeId = self.nodeId

	# Check if the node is running an attempt
	def isRunning(self):
		return (len(self.maps) + len(self.reds)) > 0

	def __str__(self):
		ret = self.nodeId
		for mapTask in self.maps:
			ret += ' ' + mapTask.attemptId
		for redTask in self.reds:
			ret += ' ' + redTask.attemptId
		return ret
