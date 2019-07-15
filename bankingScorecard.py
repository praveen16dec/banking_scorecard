import operator
import os
import sys

import bankingScorecardUtils as bs
import numpy as np
import pandas as pd
import re, itertools
from fuzzysearch import find_near_matches

MIN_CONSIDERABLE_CREDIT_AMOUNT = 100	# Minimum amount to consider for Credit Sales and Instance
MIN_REQUIRED_DAYS = 21					# Minimum days required to consider a complete month
CREDIT_SPECIFIC_DATES = [1, 5, 16, 25]
amount = {"other_lender" : []}
def processBS(bank_transaction_id, loan_code, input_od_limit = None, input_emi_amount = None):
	try:
		db, cursor = bs.getDBInstance()

			
		# ------------------------------------------------------------
		# Fetch, Filter and Tag all the transaction into appropriate category
		# Also, conisdered_year_months tells those only months for which we need to run banking scorecard
		# ------------------------------------------------------------

		all_transactions, columns = bs.getAllTransactions(bank_transaction_id, cursor)
		transaction_df, conisdered_year_months = bs.processAllTransactions(all_transactions, columns)
		transaction_df = transaction_df.where((pd.notnull(transaction_df)), None)

		db, cursor = bs.connectDB()
		final_business_name_match = ""
		final_applicant_name_match = ""
		try:
			business_name = bs.getpersondata(loan_code)[0][0]
			applicant_name = bs.getpersondata(loan_code)[0][1]

			business_name_tokens = re.split('[^a-zA-Z0-9]', business_name)
			#print business_name_tokens
			business_name_tokens = filter(None, business_name_tokens)
			applicant_name_tokens = re.split('[^a-zA-Z0-9]', applicant_name)
			applicant_name_tokens = filter(None, applicant_name_tokens)

			#print business_name_tokens, applicant_name_tokens
			if len(business_name_tokens[0] + business_name_tokens[1]) < 3:
				for each in business_name_tokens:
					final_business_name_match += each.lower()
			elif len(business_name_tokens) <= 3:
				for each in business_name_tokens:
					final_business_name_match += each.lower()
			else:
				for each in business_name_tokens[0:3]:
					final_business_name_match += each.lower()


			if len(applicant_name_tokens[0] + applicant_name_tokens[1]) < 3 or len(applicant_name_tokens[0]) < 2:
				for each in applicant_name_tokens:
					if len(each) > 1:
						final_applicant_name_match += each.lower()
			else:
				if len(applicant_name_tokens) > 1:
					for each in applicant_name_tokens[0:2]:
						final_applicant_name_match += each.lower()
				else:
					final_applicant_name_match = applicant_name_tokens[0].each

		except:
			pass

		print final_business_name_match, final_applicant_name_match
		# ------------------------------------------------------------
		# As we use a feature called (Number of Days where balance is less than x amount) for our scorecard
		# model, and in case of OD account this will come as negative. So we need to convert EOD Balance 
		# into +ve EOD balance for OD accounts. For that,
		# EOD Balance New = EOD Balance Previous + OD Limit
		# ------------------------------------------------------------

		if transaction_df is not None and conisdered_year_months != None and len(conisdered_year_months) > 0:
			wrong_paired_transactions, neg_amt_allowed_transactions, all_transaction_count = bs.validateTransactionsOrder(transaction_df)
			datewise_balance = bs.calculateEODBalance(transaction_df)
			od_utilization = []

			od_account, od_limit = False, None
			if (neg_amt_allowed_transactions * 1.0/all_transaction_count) >= 0.5:	# Threshold set for OD Account Check
				od_account = True

				# Update: Input option for OD Limit
				if input_od_limit == None or input_od_limit <= 0: od_limit = abs(min([t[1] for t in datewise_balance]))
				else: od_limit = input_od_limit

				if od_limit >= 10000:
					for d in range(len(datewise_balance)):
						datewise_balance[d] = (datewise_balance[d][0], od_limit + datewise_balance[d][1])
						od_utilization.append((datewise_balance[d][0], 100 - bs.xdiv(datewise_balance[d][1] * 100.0, od_limit)))
				else:
					od_limit = None

			print od_account
			# ------------------------------------------------------------
			# This function return ABB from 3 different approach, but we use ABB Sigma only
			# ------------------------------------------------------------

			monthly_abb_sigma, monthly_abb_median_specific_days, monthly_abb_median_all_days = bs.calculateABB(datewise_balance)

			#print monthly_abb_sigma
			monthly_recency_number = {}
			monthly_transaction_period = {}
			monthly_transaction_count = {}
			monthly_credit_instance = {}
			monthly_credit_sale = {}
			motnhly_cash_credit_sale = {}
			motnhly_cash_credit_instance = {}
			monthly_iw_bounce = {}
			monthly_iw_count = {}
			monthly_ow_bounce = {}
			monthly_ow_count = {}
			monthly_emi_bounce = {}
			monthly_unique_emi_bounce = {}
			monthly_emi_instance = {}
			motnhly_debits_lenderwise = {}

			tmp_iw_bounce = {}
			tmp_ow_bounce = {}
			tmp_emi_bounce = {}

			iw_bounced_date = {}

			pos_credit_sale = 0
			vendor_credit_sale = {}
			other_credit_sale = 0


			# ------------------------------------------------------------
			# To store sales of a customer vendor-wise
			# ------------------------------------------------------------

			for v in bs.VENDOR_NAME: vendor_credit_sale[v] = 0

			first_row = True
			previous_row = None
			previous_bounced_row = False
			bounced_row = False

			same_day_cleared_cheques = 0

			start_transaction_date, end_transaction_date = transaction_df['transactionDate'].iloc[0], transaction_df['transactionDate'].iloc[-1]
			total_transaction_period = bs.dateDifference(start_transaction_date, end_transaction_date) + 1

			print start_transaction_date, end_transaction_date
			# ------------------------------------------------------------
			# We need to know for how many period of days we have banking transactions
			# e.g. Consider a bank statement from 13 July 2018 to 27 Sept 2018, In this case
			# We have transaction of 19 Days of July, 31 Days of August and 27 Days of September
			# This is useful because we consider minimum 21 days in a month as a full month
			# ------------------------------------------------------------

			k = 1

			for m in conisdered_year_months:	# Already sorted in reverse
				monthly_recency_number[m] = k; k += 1
				yy, mm = map(int, m.split("-"))
				monthly_transaction_period[m] = bs.monthrange(yy, mm)[1]

				if start_transaction_date.year == yy and start_transaction_date.month == mm:
					monthly_transaction_period[m] -= start_transaction_date.day - 1

				if end_transaction_date.year == yy and end_transaction_date.month == mm:
					monthly_transaction_period[m] = end_transaction_date.day

				if (start_transaction_date.year == end_transaction_date.year == yy) and (start_transaction_date.month == end_transaction_date.month == mm):
					monthly_transaction_period[m] = end_transaction_date.day - start_transaction_date.day + 1 

			# try:
			# 	if not os.path.exists('./cases'):
			# 		os.makedirs('./cases')

			# 	writer = pd.ExcelWriter('./cases/' + str(bank_transaction_id) + ".xlsx")
			# 	transaction_df.to_excel(writer, 'Bankwise Summary Report')
			# 	writer.save()
			# except Exception as e:
			# 	print "Dataframe saving error", str(e)
			# 	pass

			for index, row in transaction_df.iterrows():
				is_internal = False

				try:
					tokens = re.split('[^a-zA-Z0-9]', row['description'])
					tokens = filter(None, tokens)
					full_word_description = ""
					for each in tokens:
						full_word_description += each.lower()
					if len(find_near_matches(final_applicant_name_match, full_word_description, max_l_dist=3)) <> 0:
						#print row['description']
						is_internal = True
					elif len(find_near_matches(final_business_name_match, full_word_description, max_l_dist=3)) <> 0:
						#print row['description']
						is_internal = True
				except:
					pass

				yymm = bs.date2yymm(row['transactionDate'])
				if yymm not in monthly_transaction_count:
					monthly_transaction_count[yymm] = 0
					monthly_credit_instance[yymm] = 0
					monthly_credit_sale[yymm] = 0
					motnhly_cash_credit_sale[yymm] = 0
					motnhly_cash_credit_instance[yymm] = 0
					monthly_iw_bounce[yymm] = 0
					monthly_iw_count[yymm] = 0
					monthly_ow_bounce[yymm] = 0
					monthly_ow_count[yymm] = 0
					monthly_emi_bounce[yymm] = 0
					monthly_unique_emi_bounce[yymm] = set()
					monthly_emi_instance[yymm] = set()
					motnhly_debits_lenderwise[yymm] = []

					tmp_iw_bounce[yymm] = []
					tmp_ow_bounce[yymm] = []
					tmp_emi_bounce[yymm] = []

				monthly_transaction_count[yymm] += 1


				# ------------------------------------------------------------
				# For every Credit instance we neeed to check whether this represent a Bounce or a valid credit
				# transaction (not refund or reversal) to count in credit sales
				# ------------------------------------------------------------

				if row['transactionType'] == 'credit':
					if  row['charge'] == 0 and row['transactionAmount'] >= MIN_CONSIDERABLE_CREDIT_AMOUNT:
						#print is_internal, row['description']
						if row['cash'] == 0:
							monthly_ow_count[yymm] += 1 ## check this : if this credit is coming from bounced D/C pair, reject the increment later in the loop

						######################################## Inward Bounce for OD Condition ###################################
						if not first_row :
							if not previous_bounced_row and od_account and (row['cheque2'] or previous_row['cheque2']) and \
								previous_row['transactionDate'] == row['transactionDate'] and previous_row['transactionAmount'] == row['transactionAmount'] and previous_row['transactionType'] == 'debit':
								if (row['corr'] == 0):
									bounced_row = True
									monthly_iw_bounce[yymm] += 1
									monthly_ow_count[yymm] -= 1
									#print "OD_inw", row['description'], row['transactionAmount'], row['transactionDate']

									########################### Condition for emi bounce for OD ##########################################

									if (row['lender'] + row['otherLoan'] + row['nach'] > 0)  > 0:
										#print "pair_emi_bounce", row['description'], row['transactionAmount'], row['transactionDate']
										monthly_emi_bounce[yymm] += 1
										lender_name = row['lenderName'] or previous_row['lenderName'] or str(row['transactionAmount'])
										monthly_unique_emi_bounce[yymm].add(lender_name)
									else:
										# cheque_no = row['chequeNo'].strip() or str(row['transactionAmount'])
										cheque_no = str(row['transactionAmount'])
										iw_bounced_date[cheque_no] = row['transactionDate'] # ** check this
								else:
									#print "finally here"
									monthly_ow_count[yymm] -= 1


							elif row['transactionAmount'] >= MIN_CONSIDERABLE_CREDIT_AMOUNT  and \
								(od_account or not ((row['transactionAmount'] == previous_row['transactionAmount']) and \
														(previous_row['transactionType'] == 'debit') and (row['transactionDate'] == previous_row['transactionDate']))) and \
									(row['lender'] + row['otherLoan'] == 0) and not(row['internal'] <> 0 and row['cash'] == 0) and not(is_internal and row['cash'] == 0) and (row['corr'] + previous_row['corr'] == 0): ## check this: To check whether the credit row is due to previous bounced row or not
								monthly_credit_instance[yymm] += 1
								monthly_credit_sale[yymm] += row['transactionAmount']
								#print (yymm, row['transactionAmount']), row['description']
								if row['cash'] != 0:
									#print (yymm, row['transactionAmount']), row['description']
									motnhly_cash_credit_instance[yymm] += 1
									motnhly_cash_credit_sale[yymm] += row['transactionAmount']
								elif row['vendorName'] != None:
									vendor_credit_sale[row['vendorName']] += row['transactionAmount']
								elif row['pos'] != 0: pos_credit_sale += row['transactionAmount']
								else: other_credit_sale += row['transactionAmount']

							# elif row['transactionAmount'] >= MIN_CONSIDERABLE_CREDIT_AMOUNT and \
							# 		(od_account or not ((row['transactionAmount'] == previous_row['transactionAmount']) and \
							# 				(previous_row['transactionType'] == 'debit') and (row['transactionDate'] == previous_row['transactionDate']))) and \
							# 		(row['lender'] + row['otherLoan'] > 0) and not (row['internal'] <> 0 and row['cash'] == 0) and not (is_internal and row['cash'] == 0) and (row['corr'] + previous_row['corr'] == 0):
							# 	#print "escrow", row['description'], row["transactionAmount"]
							# 	if row['lender'] > 0:
							# 		if row['lenderName']:
							# 			if row['lenderName'] not in amount:
							# 				amount[row['lenderName']] = []
							# 				amount[row['lenderName']].append(row["transactionAmount"])
							# 			else:
							# 				amount[row['lenderName']].append(row["transactionAmount"])
							# 		else:
							# 			amount['other_lender'].append(row["transactionAmount"])

							elif (row['transactionAmount'] == previous_row['transactionAmount']) and (previous_row['transactionType'] == 'debit') and (row['transactionDate'] == previous_row['transactionDate']) and not od_account and previous_bounced_row:
								monthly_ow_count[yymm] -= 1
							elif (row['transactionAmount'] == previous_row['transactionAmount']) and (previous_row['transactionType'] == 'debit') and (row['transactionDate'] == previous_row['transactionDate']) and not od_account and (row['corr']  <>  0):
								monthly_ow_count[yymm] -= 1
						else:
							if row['transactionCategory'] == "opening_balance":
								pass
							else:
								if row['transactionAmount'] >= MIN_CONSIDERABLE_CREDIT_AMOUNT and (row['lender'] + row['otherLoan'] == 0) and not(row['internal'] <> 0 and row['cash'] == 0) and not(is_internal and row['cash'] == 0):
									monthly_credit_instance[yymm] += 1
									monthly_credit_sale[yymm] += row['transactionAmount']
									#print (yymm, row['transactionAmount']), row['description']
									if row['cash'] != 0:
										#print (yymm, row['transactionAmount']), row['description']
										motnhly_cash_credit_instance[yymm] += 1
										motnhly_cash_credit_sale[yymm] += row['transactionAmount']
									elif row['vendorName'] != None:
										vendor_credit_sale[row['vendorName']] += row['transactionAmount']
									elif row['pos'] != 0:
										pos_credit_sale += row['transactionAmount']
									else:
										other_credit_sale += row['transactionAmount']


				# ------------------------------------------------------------
				# For every Debit instance we neeed to check whether this represent a Bounce or a valid debit
				# transaction (not refund or reversal) to know if this represent an EMI debit or not 
				# (If yes than how much?)
				# ------------------------------------------------------------

				else:
					if row['charge'] == 0  and row['transactionAmount'] >= MIN_CONSIDERABLE_CREDIT_AMOUNT:  ## check this : why row['charge'] == 0
						if row['cash'] == 0:
							monthly_iw_count[yymm] += 1
						try:
							similarity_val = bs.similarity_narration(row['description'], previous_row['description'])
							unique_number = bs.unique_number(row['description'], previous_row['description'])
						except:
							similarity_val = 0
							unique_number = 0
						if not first_row and not previous_bounced_row and (row['cheque2'] or previous_row['cheque2']) and \
								previous_row['transactionDate'] == row['transactionDate'] and \
								previous_row['transactionAmount'] == row['transactionAmount'] and previous_row['transactionType'] == 'credit' and (similarity_val > 0.65 or unique_number > 0):
							if row['corr']  == 0:
								#print "OD_ow", row['description'], row['transactionAmount'], row['transactionDate']
								bounced_row = True
								monthly_ow_bounce[yymm] += 1
								monthly_iw_count[yymm] -= 1
								#if (previous_row['transactionAmount'] == row['transactionAmount']) and (previous_row['transactionType'] == 'credit') and (row['transactionDate'] == previous_row['transactionDate']) : ## check this : If this is the debit row for outward bounce and there is a credit pair above it
								#monthly_ow_count[yymm] -= 1
								monthly_credit_instance[yymm] -= 1
								monthly_credit_sale[yymm] -= previous_row['transactionAmount']
								if previous_row['cash'] != 0:
									motnhly_cash_credit_instance[yymm] -= 1
									motnhly_cash_credit_sale[yymm] -= previous_row['transactionAmount']
								elif previous_row['vendorName'] != None:
									vendor_credit_sale[previous_row['vendorName']] -= previous_row['transactionAmount']
								elif previous_row['pos'] != 0:
									pos_credit_sale -= previous_row['transactionAmount']
								else:
									other_credit_sale -= previous_row['transactionAmount']
							else:
								monthly_iw_count[yymm] -= 1
								monthly_credit_sale[yymm] -= previous_row['transactionAmount']
								if previous_row['cash'] != 0:
									motnhly_cash_credit_instance[yymm] -= 1
									motnhly_cash_credit_sale[yymm] -= previous_row['transactionAmount']
								elif previous_row['vendorName'] != None:
									vendor_credit_sale[previous_row['vendorName']] -= previous_row['transactionAmount']
								elif previous_row['pos'] != 0:
									pos_credit_sale -= previous_row['transactionAmount']
								else:
									other_credit_sale -= previous_row['transactionAmount']



						if not first_row and not od_account and row['transactionAmount'] >= previous_row['balance']  and row['balance'] < 0:
							#print row# In case of wrong ordered transactions
							#print "normal_rule",  row['description'], row['transactionAmount'], row['transactionDate']
							bounced_row = True
							monthly_iw_bounce[yymm] += 1
							#monthly_ow_count[yymm] -= 1 ## check this : Only if the upcoming credit pair exists   (if only debit is given, you should not do this)

							if row['lender'] + row['otherLoan'] > 0:
								monthly_emi_bounce[yymm] += 1
								lender_name = row['lenderName'] or str(row['transactionAmount'])
								monthly_unique_emi_bounce[yymm].add(lender_name) ## check this : unique_emi_bounces
							else:
								# cheque_no = row['chequeNo'].strip() or str(row['transactionAmount'])
								cheque_no = str(row['transactionAmount'])
								iw_bounced_date[cheque_no] = row['transactionDate'] # ** Check this

						else: # check this : what does this else signify
							cheque_no = str(row['transactionAmount'])
							if cheque_no in iw_bounced_date and iw_bounced_date[cheque_no] == row['transactionDate']:
								monthly_iw_bounce[yymm] -= 1
								same_day_cleared_cheques += 1


					if row['return'] > 0 and not bounced_row:
						#print row['description']
						if row['lender'] + row['otherLoan'] + row['nach'] > 0:
							#print "emi_bouce", row['description'], row["transactionAmount"], row["transactionDate"]
							tmp_emi_bounce[yymm].append(row['description'])
						# 	lender_name = row['lenderName'] or str(row['transactionAmount'])
						# 	monthly_unique_emi_bounce[yymm].add(lender_name)
						# else:

						if row['owReturn'] > 0:
							if row['description'] not in tmp_ow_bounce[yymm]:
								tmp_ow_bounce[yymm].append(row['description'])
								#print "ow_key", row['description'], row['transactionAmount'], row['transactionDate']
						else:
							if row['description'] not in tmp_iw_bounce[yymm]:
								#print "inw_key", row['description'], row['transactionAmount'], row['transactionDate']
								tmp_iw_bounce[yymm].append(row['description'])


					if row['lender'] + row['otherLoan'] > 0 and row['transactionAmount'] >= MIN_CONSIDERABLE_CREDIT_AMOUNT:
						lender_name = row['lenderName'] or str(row['transactionAmount'])
						monthly_emi_instance[yymm].add(lender_name)  ## check this :unique_emi_instances
						motnhly_debits_lenderwise[yymm].append((lender_name, row['transactionAmount']))

				first_row = False
				previous_row = row
				previous_bounced_row = bounced_row
				bounced_row = False
			#print motnhly_cash_credit_sale
			vendor_credit_sale["pos"] = pos_credit_sale
			vendor_credit_sale["cash"] = sum(motnhly_cash_credit_sale.values())
			vendor_credit_sale["other"] = other_credit_sale
			vendor_credit_sale["bank_id"] = bank_transaction_id
			#print amount


			#print vendor_credit_sale["pos"], vendor_credit_sale["cash"], vendor_credit_sale["other"]

			tmp_abb_sigma = []
			tmp_abb_median_specific_days = []
			tmp_abb_median_all_days = []
			tmp_credit_sales = []
			tmp_cash_credit_sales = []
			tmp_cash_credit_instance = []
			tmp_credit_instance = []
			# tmp_recency_number = []
			# tmp_credit_sales2 = []

			def from_date_to_recency(dict_a, dict_b):
				new_dict = {}
				for key, val in dict_a.items():
					new_dict[dict_b[key]] = val
				return new_dict


			new_monthly_transaction_period = from_date_to_recency(monthly_transaction_period, monthly_recency_number)

			new_monthly_transaction_period_list = []
			for key, val in new_monthly_transaction_period.items():
				new_monthly_transaction_period_list.append((key, val))

			new_monthly_transaction_period_list_sorted = sorted(new_monthly_transaction_period_list, key=lambda x: x[0])


			new_monthly_abb_sigma = from_date_to_recency(monthly_abb_sigma, monthly_recency_number)
			new_monthly_abb_median_specific_days = from_date_to_recency(monthly_abb_median_specific_days, monthly_recency_number)
			new_monthly_abb_median_all_days = from_date_to_recency(monthly_abb_median_all_days, monthly_recency_number)
			new_monthly_credit_sale = from_date_to_recency(monthly_credit_sale, monthly_recency_number)
			new_monthly_cash_credit_sale = from_date_to_recency(motnhly_cash_credit_sale, monthly_recency_number)
			new_monthly_cash_credit_instance = from_date_to_recency(motnhly_cash_credit_instance, monthly_recency_number)
			new_monthly_credit_instance = from_date_to_recency(monthly_credit_instance, monthly_recency_number)
			new_monthly_debits_lenderwise = from_date_to_recency(motnhly_debits_lenderwise, monthly_recency_number)


			for k in new_monthly_transaction_period_list_sorted:
				if k[1] >= MIN_REQUIRED_DAYS:
					tmp_abb_sigma.append(new_monthly_abb_sigma[k[0]])
					tmp_abb_median_specific_days.append(new_monthly_abb_median_specific_days[k[0]])
					tmp_abb_median_all_days.append(new_monthly_abb_median_all_days[k[0]])
					#print("the total credit sale {}".format(new_monthly_credit_sale[k[0]]))
					tmp_credit_sales.append(new_monthly_credit_sale[k[0]] - new_monthly_cash_credit_sale[k[0]] + 0.7*new_monthly_cash_credit_sale[k[0]])
					tmp_cash_credit_sales.append(new_monthly_cash_credit_sale[k[0]])
					tmp_cash_credit_instance.append(new_monthly_cash_credit_instance[k[0]])
					tmp_credit_instance.append(new_monthly_credit_instance[k[0]])

				# Assuming there is only one emi per lender
				tmp_dict = {}
				if len(new_monthly_debits_lenderwise[k[0]]) != 0:
					for t in new_monthly_debits_lenderwise[k[0]]:
						if t[0] not in tmp_dict: tmp_dict[t[0]] = t[1]
						else: tmp_dict[t[0]] = max(tmp_dict[t[0]], t[1])
				new_monthly_debits_lenderwise[k[0]] = sum(tmp_dict.values())


			if len(tmp_abb_sigma) != 0:
				f1_abb_sigma = np.median(np.array(tmp_abb_sigma))
				f1_abb_median_specific_days = np.median(np.array(tmp_abb_median_specific_days))
				f1_abb_median_all_days = np.median(np.array(tmp_abb_median_all_days))
			else:
				f1_abb_sigma = 0
				f1_abb_median_specific_days = 0
				f1_abb_median_all_days = 0

			if len(tmp_credit_instance) != 0:
				f2_credit_instance = np.median(np.array(tmp_credit_instance))
			else:
				f2_credit_instance = 0
			f2_lesser_credit_instance_5 = sum(i < 5 for i in tmp_credit_instance)
			f2_lesser_credit_instance_7 = sum(i < 7 for i in tmp_credit_instance)
			f2_lesser_credit_instance_10 = sum(i < 10 for i in tmp_credit_instance)

			if len(tmp_credit_sales) != 0:
				f3_credit_sales = np.median(np.array(tmp_credit_sales))
			else:
				f3_credit_sales = 0
			f3_lesser_credit_sales_50k = sum(i < 50000 for i in tmp_credit_sales)
			f3_lesser_credit_sales_75k = sum(i < 75000 for i in tmp_credit_sales)
			f3_lesser_credit_sales_100k = sum(i < 100000 for i in tmp_credit_sales)

			if len(tmp_cash_credit_sales) != 0:
				f4_cash_credit_sales = np.median(np.array(tmp_cash_credit_sales))
			else:
				f4_cash_credit_sales = 0

			if len(tmp_cash_credit_instance) != 0:
				f4_cash_credit_instance = np.median(np.array(tmp_cash_credit_instance))
			else:
				f4_cash_credit_instance = 0

			f5_cash_credit_ratio = bs.xdiv(sum(tmp_cash_credit_sales) * 100.0, sum(tmp_credit_sales))
			f5_cash_credit_instance_ratio = bs.xdiv(sum(tmp_cash_credit_instance) * 100.0, sum(tmp_credit_instance))

			f6_emi_instances = sum(len(x) for x in monthly_emi_instance.values())
			f6_emi_bounces = sum(monthly_emi_bounce.values())
			f6_unique_emi_bounces = sum(len(x) for x in monthly_unique_emi_bounce.values())
			f6_emi_bounces_keywords = 0
			for each in tmp_emi_bounce.values():
				for text in each:
					f6_emi_bounces_keywords += 1

			tmp_debits_lenderwise = sorted(new_monthly_debits_lenderwise.items(), key = operator.itemgetter(0))
			#print tmp_debits_lenderwise
			tmp_debits_lenderwise = [t[1] for t in tmp_debits_lenderwise]

			#print tmp_debits_lenderwise
			f7_emi_amount = max(tmp_debits_lenderwise[:2]) if len(tmp_debits_lenderwise) > 0 else 0	# Last 2 month emi
			#print f7_emi_amount
			if f7_emi_amount == []:
				f7_emi_amount = 0
			# Update: Input option for EMI amount
			if input_emi_amount != None: f7_emi_amount = input_emi_amount

			f8_iw_count = sum(monthly_iw_count.values())
			f8_iw_bounce = sum(monthly_iw_bounce.values())
			f8_iw_ratio = f8_iw_bounce * 100.0 / f8_iw_count if f8_iw_count >= 100 else f8_iw_bounce
			f8_iw_bounce_keywords = 0
			for each in tmp_iw_bounce.values():
				for text in each:
					f8_iw_bounce_keywords += 1


			f9_ow_count = sum(monthly_ow_count.values())
			f9_ow_bounce = sum(monthly_ow_bounce.values())
			f9_ow_ratio = f9_ow_bounce * 100.0 / f9_ow_count if f9_ow_count >= 100 else f9_ow_bounce
			f9_ow_bounce_keywords = 0
			for each in tmp_ow_bounce.values():
				for text in each:
					f9_ow_bounce_keywords += 1

			f10_bto_trend = None
			if len(tmp_credit_sales) > 3:
				tmp_mid = (len(tmp_credit_sales) + 1)/ 2
				numerator = sum(tmp_credit_sales[:tmp_mid])
				denominator = sum(tmp_credit_sales[tmp_mid:])
				f10_bto_trend = bs.xdiv((denominator - numerator) * 100.0,  numerator)

			f13_abb_trend = None
			if len(tmp_abb_sigma) > 3:
				tmp_mid = (len(tmp_abb_sigma) + 1)/ 2
				numerator = sum(tmp_abb_sigma[:tmp_mid])
				denominator = sum(tmp_abb_sigma[tmp_mid:])
				f13_abb_trend = bs.xdiv((denominator - numerator) * 100.0,  numerator)

			f11_lesser_balance_days_2k = 0
			f11_lesser_balance_days_5k = 0
			f11_lesser_balance_days_7k = 0
			f11_lesser_balance_days_10k = 0
			for d in datewise_balance:
				if d[0].day in CREDIT_SPECIFIC_DATES:
					if d[1] < 2000: f11_lesser_balance_days_2k += 1
					if d[1] < 5000: f11_lesser_balance_days_5k += 1
					if d[1] < 7000: f11_lesser_balance_days_7k += 1
					if d[1] < 10000: f11_lesser_balance_days_10k += 1

			f12_od_median = "NA"
			f12_od_mean = "NA"
			f12_od_last30 = "NA"
			if od_account:
				tmp = [t[1] for t in od_utilization]
				f12_od_median = np.median(np.array(tmp))
				f12_od_mean = np.mean(np.array(tmp))
				f12_od_last30 = np.mean(np.array(tmp[-30:]))
			#print f8_iw_bounce, f8_iw_bounce_keywords
			tmp_inward = max(f8_iw_bounce, f8_iw_bounce_keywords)
			tmp_outward = max(f9_ow_bounce, f9_ow_bounce_keywords)

			summary_data = {
				'bank_transaction_id': bank_transaction_id,
				'start_transaction_date': str(start_transaction_date),
				'end_transaction_date': str(end_transaction_date),
				'wrong_paired_transactions': wrong_paired_transactions,
				'total_transaction_period': total_transaction_period,
				'od_account': od_account,
				'od_limit': od_limit,
				'f1_abb_sigma': f1_abb_sigma,
				'f1_abb_median_specific_days': f1_abb_median_specific_days,
				'f1_abb_median_all_days': f1_abb_median_all_days,
				'f2_credit_instance': f2_credit_instance,
				'f2_lesser_credit_instance_5': f2_lesser_credit_instance_5,
				'f2_lesser_credit_instance_7': f2_lesser_credit_instance_7,
				'f2_lesser_credit_instance_10': f2_lesser_credit_instance_10,
				'f3_credit_sales': f3_credit_sales,
				'f3_lesser_credit_sales_50k': f3_lesser_credit_sales_50k,
				'f3_lesser_credit_sales_75k': f3_lesser_credit_sales_75k,
				'f3_lesser_credit_sales_100k': f3_lesser_credit_sales_100k,
				'f4_cash_credit_sales': f4_cash_credit_sales,
				'f4_cash_credit_instance': f4_cash_credit_instance,
				'f5_cash_credit_ratio': round(f5_cash_credit_ratio, 2),
				'f5_cash_credit_instance_ratio': f5_cash_credit_instance_ratio,
				'f6_emi_instances': f6_emi_instances,
				'f6_emi_bounces': f6_emi_bounces,
				'f6_unique_emi_bounces': f6_unique_emi_bounces,
				'f6_emi_bounces_keywords': f6_emi_bounces_keywords,
				'f7_emi_amount': f7_emi_amount,
				'f8_iw_count': f8_iw_count,
				'f8_iw_bounce': tmp_inward,
				'f8_iw_ratio': f8_iw_ratio,
				'f8_iw_bounce_keywords': f8_iw_bounce_keywords,
				'f8_same_day_cleared_cheques': same_day_cleared_cheques,
				'f9_ow_count': f9_ow_count,
				'f9_ow_bounce': tmp_outward,
				'f9_ow_ratio': f9_ow_ratio,
				'f9_ow_bounce_keywords': f9_ow_bounce_keywords,
				'f11_lesser_balance_days_2k': f11_lesser_balance_days_2k,
				'f11_lesser_balance_days_5k': f11_lesser_balance_days_5k,
				'f11_lesser_balance_days_7k': f11_lesser_balance_days_7k,
				'f11_lesser_balance_days_10k': f11_lesser_balance_days_10k,
				'f12_od_median': f12_od_median,
				'f12_od_mean': f12_od_mean,
				'f12_od_last30': f12_od_last30,
				'f13_abb_trend' : f13_abb_trend,
				'monthly_credit_sales' : tmp_credit_sales[0:6],
				'monthly_abb' : tmp_abb_sigma,
				'total_cash_sale' : vendor_credit_sale['cash']
			}
			try:
				summary_data["f10_bto_trend"] = round(f10_bto_trend, 2)
			except:
				summary_data["f10_bto_trend"] = "can't analyse"

			print tmp_credit_sales[0:6]

			return summary_data, vendor_credit_sale
	except Exception as e:
		_, _, exc_tb = sys.exc_info()
		error_line = exc_tb.tb_lineno
		print "processBS", bank_transaction_id, str(e), error_line

	db.close()
	return None, None