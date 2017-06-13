from papy import Worker, Piper, Dagger, Plumber
#from reco import reco_budget, reco_freq, reco_budget_adjust, reco_crash
from numap import NuMap
import logging

#       In
#      /  \
#     /    \
#    /      \
#   OI    Report
#    |       |
#   Line     |
#    |       |
#   CP       |
#    \      /
#     \    /
#      \  /
#    RecoJoin
#      /  \
#     /    \
#    /      \
# RecoFreq RecoBudget
#    \      /
#     \    /
#      \  /
# RecoBudgetAdjust
#        |
#   SendResult

# Current produced output in console when executed:
#
# dumb: test
# reco_freq
# reco_budget
# join processing
# reco_budget_adjust
# [{'daily_budget': 150, 'days_left': 10, 'budget': 1000.0, 'report': {'cpa': 45.0, 'spend': 500}, 'bid_price': 2.0, 'daily_freq': 50, 'reco': {'budget': 995.0, 'daily_freq': 40}}]


# Workers definition
def dumb(inbox):
	# Todo simulate collector pipeline
	print 'dumb: ' + str(inbox[0]['id'])
	return {
		'budget': 1000.,
		'daily_budget': 150,
		'daily_freq': 50,
		'bid_price': 2.,
		'days_left': 10,
		'report': {
			'cpa': 45.,
			'spend': 500
		},
		'reco': {}
	}


def output_worker(inbox):
	print inbox


def reco_crash(inbox):  # Test what would happen if a step crash
	print('reco_crash')
	input = inbox[0] / 0
	return input


def reco_freq(inbox):
	print('reco_freq')
	input = inbox[0]
	input['reco']['daily_freq'] = input['daily_freq'] - 10
	return input


def reco_budget(inbox, budget_lower):
	print('reco_budget')
	input = inbox[0]
	input['reco']['budget'] = input['budget'] - budget_lower
	return input


def reco_budget_adjust(inbox):
	print('reco_budget_adjust')
	input = inbox[0]
	input['reco']['budget'] = input['reco']['budget'] + 5
	return input


# Only join recommendations since it will be the only data modified by other workers
def join_processing(inbox):
	print 'join processing'
	reco = {}
	for inp in inbox:
		reco.update(inp['reco'])
	inbox[0]['reco'] = reco
	return inbox[0]


# Program starts
if __name__ == '__main__':
	logging.info("Begin")

	pre_numap = NuMap()
	numap = NuMap()
	post_numap = NuMap()

	# Setup func
	pipeline_func = {
		'pre-processing': [dumb],  # Collector part
		#		'processing': [Worker(reco_freq), Worker(reco_budget), Worker(reco_crash)],  # Independant recos with crash
		'processing': [reco_freq, reco_budget],  # Independant recos
		'post-processing': [reco_budget_adjust]  # Adjustment recos
	}

	# Setup workers, this would be useful in the future to inject constant that would depends on user configuration
	pipeline_worker = {
		'pre-processing': [Worker(dumb)],  # Collector part
#		'processing': [Worker(reco_freq), Worker(reco_budget), Worker(reco_crash)],  # Independant recos with crash
		'processing': [Worker(reco_freq), Worker(reco_budget, (10,))],  # Independant recos
		'join_processing': Worker(join_processing),
		'post-processing': [Worker(reco_budget_adjust)],  # Adjustment recos,
		'output': [Worker(output_worker)]
	}

	# Setup pipers
	pipeline_piper = {
		'pre-processing': [Piper(worker, parallel=pre_numap, branch="pre_" + str(i)) for i, worker in enumerate(pipeline_worker['pre-processing'])],
		'processing': [Piper(worker, parallel=numap, branch=i) for i, worker in enumerate(pipeline_worker['processing'])],
		'join_processing': Piper(pipeline_worker['join_processing']),
		'post-processing': [Piper(worker, parallel=post_numap, branch="post_" + str(i)) for i, worker in enumerate(pipeline_worker['post-processing'])],
		'output': [Piper(pipeline_worker['output'][0])]
	}

#	for piper in pipeline_piper['pre-processing']:
#		piper.connect(setup)
#		piper.start()
#		print piper

	# Setup dagger
	dagger = Plumber()

	# Building topology graph
	# First build a pipe between collectors inputs and processing inputs
	# FIXME: Currently works because there's only one collector
	for pre_processing in pipeline_piper['pre-processing']:
		for processing in pipeline_piper['processing']:
			dagger.add_pipe((pre_processing, processing))

	# Pipe the processing functions to the join function
	# Join function will be responsible for producing one output
	# This will result in a single input for post processing functions
	# that are piped from the join function
	for processing in pipeline_piper['processing']:
		dagger.add_pipe((processing, pipeline_piper['join_processing']))
	for post_processing in pipeline_piper['post-processing']:
		dagger.add_pipe((pipeline_piper['join_processing'], post_processing))

	# Finally pipe the post processing functions to the output worker.
	# Output worker won't produce any further input and will save/send the data
	for post_processing in pipeline_piper['post-processing']:
		dagger.add_pipe((post_processing, pipeline_piper['output'][0]))

	#
	end = dagger.get_outputs()[0]

#	dagger.connect([setup])
	dagger.start([[{'id': 'test'}]])

	dagger.run()
	dagger.wait()
	dagger.pause()
	dagger.stop()
