#!/bin/bash

function run {
	python runsimulator.py -f demo.wl -l demo.html
	mv history.html demo.html
	python runsimulator.py -f swimDemo.wl -l swimDemo.html
	mv history.html swimDemo.html
}

run $*