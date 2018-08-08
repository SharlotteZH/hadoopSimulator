#!/usr/bin/env python

def isRealistic():
	try:
		with open('realistic'):
			return True
	except IOError:
		return False

def timeStr(seconds):
	if seconds > 60*60:
		return str(seconds/(60*60)) + 'h' + timeStr(seconds%(60*60))
	elif seconds > 60:
		return str(seconds/60) + 'm' + timeStr(seconds%60)
	else:
		return str(seconds) + 's'

def numberStr(number):
	if number > 1000*1000:
		return '%.1fM' % (number/(1000.0*1000.0))
	elif number > 1000:
		return '%.1fK' % (number/1000.0)
	else:
		return '%.1f' % number

