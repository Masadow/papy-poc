from reco import RecoBudget, RecoFreq, RecoBudgetAdjust
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
#    RecoPipe
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

if __name__ == '__main__':
	# Simulate input
	# Todo simulate collector pipeline
	setup = {
		'budget': 1000.,
		'daily_budget': 150,
		'daily_freq': 50,
		'bid_price': 2.,
		'days_left': 10,
		'report': {
			'cpa': 45.,
			'spend': 500
		}
	}

	pipeline = {
		'processing': [RecoFreq, RecoBudget],
		'pos-processing': [RecoBudgetAdjust]
	}

	#
