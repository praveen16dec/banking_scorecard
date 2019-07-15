import bankingScorecard as bsc
import bankingScorecardUtils as bs

import json
import numpy as np
import pickle
import sys
import itertools
import time


# ------------------------------------------------------------
# Package for interpreting scikit-learn's decision tree and random forest predictions
# https://blog.datadive.net/random-forest-interpretation-with-scikit-learn/
# ------------------------------------------------------------

from treeinterpreter import treeinterpreter as ti

AIKEY = 'hocpsjmgebqxudrykzitvwanlf'
FEATURES = ['f1_abb_sub_emi', 'f2_inward', 'f3_otward', 'f4_emi_instances', 'f5_unique_emi_bounces', 'f6_cash_credit_ratio', 'f7_credit_instance', 'f8_credit_sales', 'f9_bto_trend', 'f10_lesser_credit_instance_5', 'f11_lesser_credit_instance_7', 'f12_lesser_credit_instance_10', 'f13_lesser_credit_sales_50k', 'f14_lesser_credit_sales_75k', 'f15_lesser_credit_sales_100k', 'f16_lesser_balance_days_2k', 'f17_lesser_balance_days_5k', 'f18_lesser_balance_days_7k', 'f19_lesser_balance_days_10k']


# ------------------------------------------------------------
# This function consider feature vector as input calculated for a bank statement. To get features,
# use processBS function of bankingScorecard.py File. After that, we will pass these features to
# already trained models for predictivity and model interpretation
# ------------------------------------------------------------

def predictResult(feature_vector):
	try:
		predictions = []	# As there are multiple trained models
		result = None		# Final result after combining all the models

		feature_vector = [feature_vector]	# 2D input array required
		feature_contributuon = {}			# Contribution of each feature for that score

		for f in FEATURES: feature_contributuon[f] = 0


		# ------------------------------------------------------------
		# Load Previously trained models and predict the result for each
		# For overall result calculate the average of all and for confidence calculate the variance

		# As, we are using multiple models and their combination, so to combine every model we need to equalize 
		# base (neutral) score from tree-interpreter. This can be done by normal scaling.
		# ------------------------------------------------------------

		for i in range(1, 6):
			model = pickle.load(open("bankingApprovalModelNew" + str(i) + ".sav", 'rb'))
			predictions.append(model.predict_proba(feature_vector)[:, 1][0])

			prediction, bias, contributions = ti.predict(model, np.array(feature_vector))
			prediction = prediction[0][1] * 100
			bias = bias[0][1] * 100
			bias_deviation = (50 - bias)	# deviation from base value (50)

			contributions = contributions[0][:, 1]
			contributions = [x*100 for x in contributions]

			acs = sum([abs(x) for x in contributions])
			bias_deviation_ratio = bias_deviation / acs


			# ------------------------------------------------------------
			# As we are shifting base score to 0.5, so the deviation has to managed into other 
			# features as per their contribution (In proportion)
			# ------------------------------------------------------------

			for i in range(len(contributions)): contributions[i] = contributions[i] - abs(contributions[i]) * bias_deviation_ratio
			for x, y in zip(FEATURES, contributions): feature_contributuon[x] += y


		# ------------------------------------------------------------
		# Average contribution from each features
		# Average score and vaariance of predictions (confidence)
		# ------------------------------------------------------------

		for f in feature_contributuon: feature_contributuon[f] = round(feature_contributuon[f] / 5.0, 2)

		result = int(np.mean(predictions) * 100)
		confidence = int(max(np.std(predictions) * 100, 100))
		return result, confidence, predictions, feature_contributuon

	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		error_line = exc_tb.tb_lineno
		print "Error in Predict Result", str(e), str(error_line)
		return None, None, None, None


# ------------------------------------------------------------
# This is the main input function, which get called after API request
# This handles everythiing from feature calculation to passing it to model. Also,
# make a structured json format for API response
# ------------------------------------------------------------

def startApplication(bank_transaction_id, loan_code, input_od_limit = None, input_emi_amount = None):

	try:
		# ------------------------------------------------------------
		# Pass bank account id to processBS function, which retures 4 parameters
		# Summary data - summary points of banking data
		# Vendor Credit Sales - sales of the customer vendor wise (eg. flipkart, pos, cash etc.)
		# feature data - feature data considered for modeling
		# feature_vector - A vector of feature data (directly can be used for prediction)
		# ------------------------------------------------------------

		summary_data, vendor_credit_sale = bsc.processBS(bank_transaction_id, loan_code, input_od_limit, input_emi_amount)
		#feature_data, feature_vector

	except:
		summary_data = {}
		vendor_credit_sale = {}
		pass

	return summary_data, vendor_credit_sale

def summary_to_feature(summary):
	summary_data_collection = {}
	feature_vector_collection = {}
	feature_data_collection = {}
	## Write itertools for selection
	print len(summary)
	#for i in range(1,len(summary)+1):
	comb_list = list(itertools.combinations(summary,len(summary)))
	print len(comb_list)
	for comb in comb_list:
		key = ""
		summary_data = {"monthly_credit_sales" : [], "total_transaction_period" : [], "monthly_abb" : []}
		for each in comb:
			key += str(each['bank_transaction_id']) + "-"
			maximum = 0
			for in_key, in_val in each.items():
				if in_key not in ["bank_transaction_id", "start_transaction_date", "end_transaction_date", "monthly_abb", "total_transaction_period", "od_account", "od_limit", "f1_abb_sigma", "f1_abb_median_all_days", "f13_abb_trend", "f12_od_last30", "f12_od_mean", "f12_od_median", "f9_ow_ratio", "f8_iw_ratio", "f5_cash_credit_ratio", "f5_cash_credit_instance_ratio", "monthly_credit_sales", "bto_trend"]:
					if in_key not in summary_data:
						summary_data[in_key] = in_val
					else:
						summary_data[in_key] += in_val
				if in_key == "f1_abb_median_specific_days":
					if each['f3_credit_sales'] > maximum:
						maximum = each['f3_credit_sales']
						summary_data[in_key] = in_val
						summary_data['monthly_abb'] = each['monthly_abb']
				if in_key == 'monthly_credit_sales':
					summary_data[in_key].append(in_val)
				if in_key == "total_transaction_period":
					summary_data[in_key].append(in_val)

		if 'f1_abb_median_specific_days' not in summary_data:
			summary_data['f1_abb_median_specific_days'] = 0

		a = [sum(x) for x in zip(*summary_data['monthly_credit_sales'])]
		summary_data['monthly_credit_sales'] = a


		#print summary_data['monthly_abb']

		if len(a) > 3:
			tmp_mid = (len(a) + 1) / 2
			numerator = sum(a[:tmp_mid])
			denominator = sum(a[tmp_mid:])
			f10_bto_trend = bs.xdiv((denominator - numerator) * 100.0, numerator)
		else:
			f10_bto_trend = None

		summary_data['f10_bto_trend'] = f10_bto_trend

		summed = 0
		count = 0
		for i, each in enumerate(summary_data['total_transaction_period']):
			summed += each
			count += 1
		summary_data['total_transaction_period'] = bs.xdiv(summed, count)

		summary_data['f5_cash_credit_ratio'] = bs.xdiv(summary_data['f4_cash_credit_sales'], summary_data['f3_credit_sales'])
		summary_data['f5_cash_credit_instance_ratio'] = bs.xdiv(summary_data['f4_cash_credit_instance'], summary_data['f2_credit_instance'])

		## calculate inward and outward return ratios
		if summary_data['f8_iw_count'] > 100:
			summary_data['inward_ratio'] = round(summary_data['f8_iw_bounce'] * 1.0 * 100 / summary_data['f8_iw_count'],0)
		else:
			if summary_data['f8_iw_bounce'] > 100:
				summary_data['inward_ratio'] = 100
			else:
				summary_data['inward_ratio'] = summary_data['f8_iw_bounce']

		if summary_data['f9_ow_count'] > 100:
			summary_data['outward_ratio'] = round(summary_data['f9_ow_bounce'] * 1.0 * 100/ summary_data['f9_ow_count'], 0)
		else:
			if summary_data['f9_ow_bounce'] > 100:
				summary_data['outward_ratio'] = 100
			else:
				summary_data['outward_ratio'] = summary_data['f9_ow_bounce']

		feature_data = {
			'f1_abb_sub_emi': summary_data['f1_abb_median_specific_days'] - summary_data['f7_emi_amount'],
			'f2_inward': summary_data['inward_ratio'],
			'f3_outward': summary_data['outward_ratio'],
			'f4_emi_instances': summary_data['f6_emi_instances'],
			'f5_unique_emi_bounces': summary_data['f6_unique_emi_bounces'],
			'f6_cash_credit_ratio': summary_data['f5_cash_credit_ratio'],
			'f7_credit_instance': summary_data['f2_credit_instance'],
			'f8_credit_sales': summary_data['f3_credit_sales'],
			'f9_bto_trend': summary_data['f10_bto_trend'],
			'f10_lesser_credit_instance_5': summary_data['f2_lesser_credit_instance_5'],
			'f11_lesser_credit_instance_7': summary_data['f2_lesser_credit_instance_7'],
			'f12_lesser_credit_instance_10': summary_data['f2_lesser_credit_instance_10'],
			'f13_lesser_credit_sales_50k': summary_data['f3_lesser_credit_sales_50k'],
			'f14_lesser_credit_sales_75k': summary_data['f3_lesser_credit_sales_75k'],
			'f15_lesser_credit_sales_100k': summary_data['f3_lesser_credit_sales_100k'],
			'f16_lesser_balance_days_2k': round(summary_data['f11_lesser_balance_days_2k'] * 100.0 / summary_data['total_transaction_period'], 2),
			'f17_lesser_balance_days_5k': round(summary_data['f11_lesser_balance_days_5k'] * 100.0 / summary_data['total_transaction_period'], 2),
			'f18_lesser_balance_days_7k': round(summary_data['f11_lesser_balance_days_7k'] * 100.0 / summary_data['total_transaction_period'], 2),
			'f19_lesser_balance_days_10k': round(summary_data['f11_lesser_balance_days_10k'] * 100.0 / summary_data['total_transaction_period'], 2)
		}

		feature_vector = [feature_data['f1_abb_sub_emi'],  feature_data['f2_inward'], feature_data['f3_outward'], feature_data['f4_emi_instances'], feature_data['f5_unique_emi_bounces'], feature_data['f6_cash_credit_ratio'], feature_data['f7_credit_instance'], feature_data['f8_credit_sales'], feature_data['f9_bto_trend'], feature_data['f10_lesser_credit_instance_5'], feature_data['f11_lesser_credit_instance_7'], feature_data['f12_lesser_credit_instance_10'], feature_data['f13_lesser_credit_sales_50k'], feature_data['f14_lesser_credit_sales_75k'], feature_data['f15_lesser_credit_sales_100k'], feature_data['f16_lesser_balance_days_2k'], feature_data['f17_lesser_balance_days_5k'], feature_data['f18_lesser_balance_days_7k'], feature_data['f19_lesser_balance_days_10k']]

		summary_data_collection[key] = summary_data
		feature_data_collection[key] = feature_data
		feature_vector_collection[key] = feature_vector

	return summary_data_collection, feature_data_collection, feature_vector_collection


# # --------- Flask code begins ---------
# from flask import Flask
# from flask import request
# from flask import jsonify
#
# app = Flask(__name__)
#
# @app.route("/bankingModel", methods = ['GET'])
# def gteBankingScore():
# 	body = request.get_data()
# 	header = request.headers
#
# 	start_time = time.time()
#
# 	bank_transaction_id = None
# 	loan_code = None
#
# 	try:
# 		if 'aikey' in header and header['aikey'] == AIKEY:
# 			input_od_limit = None
# 			input_emi_amount = None
#
# 			if 'bank_account_summary_id' in request.args:
# 				bank_transaction_id = str(request.args['bank_account_summary_id'])
#
# 			if 'loan_code' in request.args:
# 				loan_code = str(request.args['loan_code'])
#
# 			if 'odLimit' in request.args:
# 				input_od_limit = int(request.args['odLimit'])
#
# 			if 'emiAmount' in request.args:
# 				input_emi_amount = int(request.args['emiAmount'])
#
# 			if bank_transaction_id != None and loan_code != None:
# 				res = startApplication(bank_transaction_id, input_od_limit, input_emi_amount)
# 			else:
# 				res = {
# 					'success': False,
# 					'message': 'Bank account Summary Id not found'
# 				}
# 		else:
# 			res = {
# 				'success': False,
# 				'message': 'Key not found or wrong key'
# 			}
# 	except Exception as e:
# 		res = {
# 			'success': False,
# 			'message': str(e)
# 		}
#
# 	end_time = time.time()
#
# 	processTime = round(end_time - start_time, 2)
# 	version = '0.1.0'
#
# 	if 'result' in res and res['result'] != None and res['result'] != None:
# 		res['result']['processTime'] = processTime
# 		res['result']['version'] = version
#
# 	if bank_transaction_id != None:
# 		res['bank_transaction_id'] = bank_transaction_id
#
# 	if loan_code != None:
# 		res['loan_code'] = loan_code
#
# 	score = None
# 	if 'result' in res and res['result'] != None and 'score' in res['result']:
# 		score = res['result']['score']
#
# 	try:
# 		if score != None and 'success' in res and res['success']:
# 			db, cursor = bsu.getDBInstance()
#
# 			loan_code = res['loan_code']
# 			bank_account_summary_id = res['bank_transaction_id']
# 			score = score
# 			score_data = json.dumps(res['result'])
#
#
# 			# ------------------------------------------------------------
# 			# Insert or update the result into database
# 			# ------------------------------------------------------------
#
# 			try:
# 	   			cursor.execute(""" INSERT INTO bank_db.banking_scorecard (loan_code, bank_account_summary_id, score, score_data) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE  score = %s, score_data = %s""",  (loan_code, bank_account_summary_id, score, score_data, score, score_data))
# 	   			db.commit()
# 			except Exception as e:
# 				print "Exception error", str(e)
# 				pass
#
# 	except Exception as e:
# 		print "Exception error", str(e)
# 		pass
#
# 	return jsonify(res)
#
# if __name__ == "__main__":
# 	app.run(debug = True, host = "0.0.0.0", port = 7741)