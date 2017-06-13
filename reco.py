from papy import Worker


class Reco:
	# Default behavior simply return the its input
	# This won't break the chain if not implemented
	def process(self, inbox):
		input = inbox[0]
		return input

	def __init__(self):
		self.w = Worker([self.process])


def reco_crash(inbox):  # Test what would happen if a step crash
	print('reco_crash')
	input = inbox[0] / 0
	return input


def reco_freq(inbox):
	print('reco_freq')
	input = inbox[0]
	return input


def reco_budget(inbox):
	print('reco_budget')
	input = inbox[0]
	return input


def reco_budget_adjust(inbox):
	print('reco_budget_adjust')
	input = inbox[0]
	return input
