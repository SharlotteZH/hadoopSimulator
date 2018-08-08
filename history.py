#!/usr/bin/env python

from job import ID
from job import Job
from job import Task
from job import Attempt

from commons import timeStr

from math import floor, ceil
from operator import attrgetter
from operator import itemgetter

'''
Store a simulation history.
'''
class History:
	def __init__(self, filename='history.log'):
		self.filename = filename
		if self.filename != None:
			self.file = open(self.filename, 'w')

	def __del__(self):
		self.close()

	def close(self):
		if self.filename != None:
			self.file.close()

	def getFilename(self):
		return self.filename

	def logJob(self, job):
		if self.filename != None:
			self.file.write('Job JOBID="%s" JOB_STATUS="%s" SUBMIT_TIME="%d" START_TIME="%d" FINISH_TIME="%d" .\n' % (job.jobId, Job.Status.toString[job.status], job.submit, job.getStart(), job.getFinish()))

	def logTask(self, task):
		if self.filename != None:
			self.file.write('Task TASKID="%s" TASK_STATUS=%s" .\n' % (task.taskId, Job.Status.toString[task.status]))

	def logAttempt(self, attempt):
		if attempt.isMap():
			self.logMapAttempt(attempt)
		else:
			self.logReduceAttempt(attempt)

	def logMapAttempt(self, attempt):
		if self.filename != None:
			self.file.write('MapAttempt TASKID="%s" TASK_ATTEMPT_ID="%s" TASK_STATUS="%s" START_TIME="%d" FINISH_TIME="%d" HOSTNAME="%s" .\n' % (attempt.getTaskId(), attempt.attemptId, attempt.status, attempt.start, attempt.finish, attempt.nodeId))

	def logReduceAttempt(self, attempt):
		if self.filename != None:
			self.file.write('ReduceAttempt TASKID="%s" TASK_ATTEMPT_ID="%s" TASK_STATUS="%s" START_TIME="%d" FINISH_TIME="%d" HOSTNAME="%s" .\n' % (attempt.getTaskId(), attempt.attemptId, attempt.status, attempt.start, attempt.finish, attempt.nodeId))

	def logNodeStatus(self, t, node):
		if self.filename != None:
			self.file.write('Node HOSTNAME="%s" TIME="%d" STATUS="%s" .\n' % (node.nodeId, t, node.status))


'''
Generate an HTML with the trace.
'''
class HistoryViewer:
	def __init__(self, filenamein='history.log', filenameout='history.html'):
		self.filenamein  = filenamein
		self.filenameout = filenameout
		if self.filenamein != None and self.filenameout != None:
			self.filein =  open(self.filenamein,  'r')
			self.fileout = open(self.filenameout, 'w')
			#self.zoom = 0.5
			self.zoom = 0.2

			self.plotJobs = True
			self.plotTasks = False
			self.plotNodeTasks = False
			self.plotNodes = True
			self.plotNodesStatus = True

	def __del__(self):
		self.close()

	def close(self):
		if self.filenamein != None and self.filenameout != None:
			self.filein.close()
			self.fileout.close()

	def getDict(self, strings):
		ret = {}
		for keyvalue in strings:
			key, value = keyvalue.split('=')
			while value.startswith('"'):
				value = value[1:]
			while value.endswith('"'):
				value = value[:-1]
			ret[key] = value
		return ret

	def getTaskGraph(self, attempt):
		content = str(ID.getId(attempt.getTaskId()))
		content = ''
		out =  '<table  border="0" cellspacing="0" cellpadding="0" style="border:1px solid:black;">';
		out += '<tr height="5px">\n';
		out += '<td width="' + str(floor(1.0*                attempt.start *self.zoom)) + 'px" bgcolor="white"/>\n'
		out += '<td width="' + str(floor(1.0*(attempt.finish-attempt.start)*self.zoom)) + 'px" style="background-color:'+self.getTaskColor(attempt)+'; color:#FFFFFF; font-size:small" align="left">'+content+'</td>\n'
		out += '</tr>'
		out += '</table>\n'
		return out

	def getTaskGraphList(self, attempts):
		out =  '<table  border="0" cellspacing="0" cellpadding="0">'# style="border:1px solid black;">'
		#out += '<tr height="20px">\n'
		out += '<tr height="5px">\n'
		prev = 0
		for attempt in attempts:
			content = str(ID.getId(attempt.getTaskId()))
			content = ''
			out += '<td width="' + str(floor(1.0*(attempt.start -   prev)*self.zoom)) + 'px" bgcolor="white"/>\n'
			out += '<td width="' + str(floor(1.0*(attempt.finish-attempt.start)*self.zoom)) + 'px" style="background-color:'+self.getTaskColor(attempt)+'; color:#FFFFFF; font-size:small; border-color:black; border:1px solid:black; border-style: inset; border-width:1px;" align="left">'+content+'</td>\n'
			prev = attempt.finish
		out += "</tr>";
		out += "</table>\n";
		return out

	def getJobGraph(self, job):
		content = ''
		out =  '<table  border="0" cellspacing="0" cellpadding="0">';
		out += '<tr height="5px">\n';
		out += '<td width="' + str(floor(1.0*                job.submit *self.zoom)) + 'px" bgcolor="white"/>\n'
		out += '<td width="' + str(floor(1.0*(job.start-job.submit)*self.zoom)) + 'px" style="background-color:#0000FF; color:#FFFFFF; font-size:small; border-color:black; border:1px solid:black; border-style: inset; border-width:1px;" align="left">'+content+'</td>\n'
		out += '<td width="' + str(floor(1.0*(job.finish-job.start)*self.zoom)) + 'px" style="background-color:#00FF00; color:#FFFFFF; font-size:small; border-color:black; border:1px solid:black; border-style: inset; border-width:1px;" align="left">'+content+'</td>\n'
		out += '</tr>'
		out += '</table>\n'
		return out

	def getNodeStatusGraphList(self, nodeStatuses):
		out =  '<table  border="0" cellspacing="0" cellpadding="0">'# style="border:1px solid black;">'
		out += '<tr height="15px">\n'
		prevNodeStatus = nodeStatuses[0]
		for nodeStatus in nodeStatuses[1:]:
			out += '<td width="' + str(floor(1.0*(nodeStatus[0] - prevNodeStatus[0])*self.zoom)) + 'px" bgcolor="'+self.getNodeColor(prevNodeStatus[1])+'"/>\n'
			prevNodeStatus = nodeStatus
		out += "</tr>";
		out += "</table>\n";
		return out

	def getNodeColor(self, nodeStatus):
		if nodeStatus == 'ON':
			color = "#00FF00"
		elif nodeStatus == 'SLEEP':
			color = "#FFFFFF"
		elif nodeStatus.startswith('SLEEPING'):
			color = "#800000"
		elif nodeStatus.startswith('WAKING'):
			color = "#008000"
		elif nodeStatus.startswith('OFF'):
			color = "#000000"
		else:
			color = "#101010"
		return color

	def getTaskColor(self, attempt):
		if attempt.isMap():
			color = "#000080"
		elif attempt.isRed():
			color = "#800000"
		else:
			color = "#008000"
		return color

	'''
	Generate execution report from history file.
	'''
	def generate(self):
		if self.filenamein != None and self.filenameout != None:
			# Read all the information
			jobs = {}
			attempts = []
			nodes = {}
			for line in self.filein.readlines():
				if line.startswith('Job'):
					ret = self.getDict(line.split(' ')[1:-1])
					jobId = ret['JOBID']
					if jobId not in jobs:
						jobs[jobId] = Job(jobId=jobId)
					jobs[jobId].status = ret['JOB_STATUS']
					jobs[jobId].submit = int(ret['SUBMIT_TIME'])
					jobs[jobId].start =  int(ret['START_TIME'])
					jobs[jobId].finish = int(ret['FINISH_TIME'])
				elif line.startswith('Task'):
					pass
				elif line.startswith('MapAttempt') or line.startswith('ReduceAttempt'):
					ret = self.getDict(line.split(' ')[1:-1])
					attempt = Attempt()
					attempt.attemptId = ret['TASK_ATTEMPT_ID']
					attempt.start =  int(ret['START_TIME'])
					attempt.finish = int(ret['FINISH_TIME'])
					attempt.status = ret['TASK_STATUS']
					attempt.nodeId = ret['HOSTNAME']
					attempts.append(attempt)
					# Update job information
					jobId = attempt.getJobId()
					if jobId not in jobs:
						jobs[jobId] = Job(jobId=jobId)
					jobs[jobId].addAttempt(attempt)
				elif line.startswith('Node'):
					ret = self.getDict(line.split(' ')[1:-1])
					nodeId = ret['HOSTNAME']
					if nodeId not in nodes:
						nodes[nodeId] = []
					nodes[nodeId].append((int(ret['TIME']), ret['STATUS']))

			# Node -> Attempts
			nodeAttempts = {}
			for attempt in attempts:
				if attempt.nodeId not in nodeAttempts:
					nodeAttempts[attempt.nodeId] = []
				nodeAttempts[attempt.nodeId].append(attempt)
			if 'None' in nodeAttempts:
				del nodeAttempts['None']

			totalJobTime = 0
			totalJobRunTime = 0
			for jobId in jobs:
				job = jobs[jobId]
				totalJobTime += job.finish - job.submit
				totalJobRunTime += job.finish - job.start
			totalJobTime = totalJobTime/len(jobs)
			totalJobRunTime = totalJobRunTime/len(jobs)

			# Generate output HTML
			self.fileout.write('<html>\n')
			self.fileout.write('<head>\n')
			self.fileout.write('<link rel="stylesheet" type="text/css" href="http://aws018:50030/static/hadoop.css">\n')
			self.fileout.write('<title>Execution profile</title>\n')
			self.fileout.write('</head>\n')
			self.fileout.write('<body>\n')

			# Summary
			self.fileout.write('<h1>Summary</h1>\n')
			self.fileout.write('<ul>\n')
			self.fileout.write('  <li>Jobs: %d</li>\n' % len(jobs))
			self.fileout.write('  <ul>\n')
			self.fileout.write('    <li>Average turn-around time: %.1fs</li>\n' % totalJobTime)
			self.fileout.write('    <li>Average runtime: %.1fs</li>\n' % totalJobRunTime)
			self.fileout.write('  </ul>\n')
			self.fileout.write('  <li>Attempts: %d</li>\n' % len(attempts))
			self.fileout.write('</ul>\n')

			# Jobs
			if self.plotJobs:
				self.fileout.write('<h1>Jobs</h1>\n')
				self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
				self.fileout.write('<tr><th width="100px">Id</th><th width="100px">Quality</th><th width="100px">Time</th></tr>\n')
				for job in sorted(jobs.values(), key=attrgetter('finish')):
					self.fileout.write('<tr><td>%s</td><td align="right">%.1f%%</td><td align="right">%d+%ds &nbsp;</td><td>%s</td></tr>\n' %(job.jobId, 100, job.start-job.submit, job.finish-job.start, self.getJobGraph(job)))
				self.fileout.write('</table>\n')

			# Tasks
			if self.plotTasks:
				self.fileout.write('<h1>Tasks</h1>\n')
				self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
				for attempt in sorted(attempts, key=attrgetter('attemptId')):
					self.fileout.write('<tr><td>'+self.getTaskGraph(attempt)+'</td></tr>\n')
				self.fileout.write('</table>\n')

			# Nodes -> Tasks
			if self.plotNodeTasks:
				self.fileout.write('<h1>Node &rarr; Tasks</h1>\n')
				self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
				for nodeId in sorted(nodeAttempts):
					self.fileout.write('<tr><td valign="top">'+nodeId+'</td>')
					self.fileout.write('<td>')
					self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
					for attempt in sorted(nodeAttempts[nodeId], key=attrgetter('start', 'attemptId')):
						self.fileout.write('<tr><td>'+self.getTaskGraph(attempt)+'</td></tr>\n')
					self.fileout.write('</table>\n')
					self.fileout.write('</td>')
					self.fileout.write('</tr>')
				self.fileout.write('</table>\n')

			# Nodes
			if self.plotNodes:
				self.fileout.write('<h1>Nodes</h1>\n')
				self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
				for nodeId in sorted(nodeAttempts):
					self.fileout.write('<tr><td valign="top">'+nodeId+'</td>')
					self.fileout.write('<td>')
					self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
					# Create slots
					slots = []
					for attempt in nodeAttempts[nodeId]:
						added = False
						for slot in slots:
							if slot[-1].finish <= attempt.start:
								slot.append(attempt)
								added = True
								break
						if not added:
							# Every slot is full, create a new one
							slots.append([attempt])
					# Draw slots
					for slot in slots:
						self.fileout.write('<tr><td>'+self.getTaskGraphList(slot)+'</td></tr>')
					self.fileout.write('</table>\n')
					self.fileout.write('</td>')
					self.fileout.write('</tr>')
				self.fileout.write('</table>\n')

			# Nodes ON/OFF
			if self.plotNodesStatus:
				self.fileout.write('<h1>Nodes status</h1>\n')
				self.fileout.write('<table border="0" cellspacing="0" cellpadding="0">\n')
				for nodeId in sorted(nodes):
					self.fileout.write('<tr><td valign="top">'+nodeId+'</td>') # nodes[nodeId]
					self.fileout.write('<td>'+self.getNodeStatusGraphList(nodes[nodeId])+'</td></tr>')
				self.fileout.write('</table>\n')

			self.fileout.write('</body>\n')
			self.fileout.write('</html>\n')
