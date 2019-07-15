from calendar import monthrange
from datetime import date, timedelta
import re
import sys

#import MySQLdb
import pymysql

import numpy as np
import pandas as pd

from difflib import SequenceMatcher
import re, itertools


CREDIT_SPECIFIC_DATES = [1, 5, 16, 25]	# To Calculate Credit ABB
MIN_CONSIDERABLE_CREDIT_AMOUNT = 100	# Minimum amount to consider for Credit Sales and Instance
MONTH = 30								# Days in a Month


# ------------------------------ Loan Keywords ------------------------------
LENDER = ["aadri", "adanicap", "adityabirla","aditybirla", "bajaj", "bajranginvestment", "berarfin", "bluejay", "capitalfirst", "caprigl", "clixcap", "dewanhous", "eclfin", "edelweiss", "epimoney", "equitashous", "flexiloans", "fullerton", "fullindhfin", "fulllerton", "hdbdisb", "hdbf", "hdfcltb", "herofincorp", "homecredit", "homecrindfin", "indiabull", "indiainfoline", "indifi", "indinffinltd", "janalakshmifin", "kotakmahindra", "l&tfin", "l&thousingfin", "lendingkart", "loantap", "magmaemi", "magmafin", "mahfinan", "mahindrafin", "neogrowth", "rivierainvest", "shriramcityunion", "tatacap", "unitedpetro", "unitedpetrofinance", "urmilainvest", "visagehol", "zenlefin"]
LENDER_NAME = ["lendingkart", "adanicapital", "adityabirla", "adityabirla", "bajajfinserv", "loantap", "berarfinance", "ziploan", "capitalfirst", "capricapital", "clixcapital", "dhfl", "edelweiss", "edelweiss", "flexiloans", "equitashousing", "flexiloans", "fullerton", "fullertonhousing", "fullerton", "hdb", "hdb", "hdfc", "herofincorp", "homecredit", "homecredit", "indiabullsfinance", "indiainfoline", "indifi", "janalakshmifinancial", "kotak", "l&t", "thousingfinance", "lendingkart", "loantap", "magmafincorp", "magmafincorp", "mahindrafinance", "mahindrafinance", "neogrowth", "rivierafinance", "shriramcityunion", "tatacapital", "unitedpetrofinance", "unitedpetrofinance", "flexiloans", "kinaracapital", "capitalfloat"]
OTHER_LOAN = ["emi", "loan", "installment"]



# ------------------------------ Vendor Keywords ------------------------------
VENDOR = ["flipkartindia", "cloudtail", "myn5c34fe6a9fb99tra", "appario", "furlenco", "trendsutra", "flipkart", "craftsvilla", "paytm", "one97", "zomato", "amazon", "jasper", "ammarketplace", "clues", "bundl", "ebay", "voonik", "rediff", "mintkart", "jabong", "novarris", "reliance", "alixepr", "shopify", "tv18", "flipkar", "askme", "citruspay", "hiveloop", "justdial", "mobikwik", "oravel", "uber"]
VENDOR_NAME = ["flipkartindia", "cloudtail", "myntra", "appario", "furlenco", "trendsutra", "flipkart", "craftsvilla", "paytm", "paytm", "zomato", "amazon", "snapdeal", "limeroad", "shopclues", "swiggy", "ebay", "voonik", "rediff", "ebay", "jabong", "jabong", "reliance", "aliexpress", "shopify", "homeshop18", "flipkart", "askme", "citruspay", "hiveloop", "justdial", "mobikwik", "oyo", "uber"]


# ------------------------------ Return / Charge Keywords ----------------------------
CHARGES = ["charge", "chrg", "chg", "chgs", "amc", "fee", "gst"]
RETURN = ["ret", "rtn", "return", "fundinsufficient", "fundsinsufficient", "insufficientfund", "insufficientfunds", "iwrtn", "owrtn"]
INWARD_RETURN = ["inward", "iwrtn", "iw", "i/w"]
OUTWARD_RETURN = ["outward", "owrtn", "ow", "out", "o/w"]
CORRECTED	= ["corr", "cor" ]



# ---------------------------- Payment Keywords -----------------------------
CHEQUE = ["chq", "cheque", "cheq", "clearing", "clg"]
CASH = ["atm", "cash", "csh", "atl", "wdl", "nfs", "atw", "cshdep", "cshwdl", "eaw", "nwd", "owd", "ats", "awb"]
NACH = ["ecs", "ach", "nach"]
NEFT = ["neft"]
IMPS = ["imps", "upi"]
RTGS = ["rtgs"]
TPT = ["tpt"]
POS = ["payu", "mswipe", "paynearby", "payzapp", "razorpay", "pinelabs", "mindsarray", "instamoj", "pos", "terminal"]
WALLET = ["paytm", "one97", "mobikwik", "mobikwk", "payu", "freecharge"]
UTILITY_BILL = ["bill", "billdesk", "billpay", "billpay", "dth", "gas", "telephone", "pos", "power", "broadband", "axmob", "vodafone", "airtel", "tata", "reliance", "idea", "bsnl", "mtnl", "aircel", "creditcard", "debitcard", "autopay", "sbicard", "card payment", "cc"]
INTERNAL = ["self", "internal", "family", "uncle", "sister", "brother", "father", "mother", "investment", "capital", "salary"]

PAYMENT_CATEGORY = {
	'debit': -1,
	'credit': 1
}


# ----------------------------------------------------
# Get the DB Instance for Dev and Host Environment Both
# Don't forget to close the connection after use
# ----------------------------------------------------

def getDBInstance():
	host = "db-instance-replica.ckeu8iwmmeka.ap-south-1.rds.amazonaws.com"
	# host = "104.211.216.91"	# dev environment
	db = "bank_db"
	user = "praveen_y"
	password = "edfghkRCasdwsdfoj123"
	# user = "bank_app"
	# password = "YTFDasdfrtgfdwss21!2"

	db = pymysql.connect(host = host, db = db, user = user, passwd = password)
	cursor = db.cursor()
	return db, cursor


def connectDB(db="flexiloans_aws_prod_db"):
	host = "db-instance-replica.ckeu8iwmmeka.ap-south-1.rds.amazonaws.com"
	db = db
	user = "ds_doc_user"
	password = "TGe@reR!D3478i"

	db = pymysql.connect(host=host, db=db, user=user, passwd=password)
	cursor_2 = db.cursor()

	return cursor_2, db

def getpersondata(loan_code):
	cursor_2, db = connectDB()

	cursor_2.execute('select lbd.business_name, lad.name from loan_business_detail lbd left join loan_applicant_detail lad on lad.loan_code = lbd.loan_code where lbd.loan_code = %s', loan_code)

	result = cursor_2.fetchall()
	db.close()

	return result


def account_type(statement_id):
	db, cursor = getDBInstance()

	cursor.execute("select account_type_det from bank_account_summary where id = %s", statement_id)
	result = cursor.fetchall()

	db.close()
	return result

def xdiv(a, b):
	return 0 if b == 0 else a * 1.0 / b


def dateDifference(d1, d2):
	return abs((d1 - d2).days)


def similar(a, b):
	return SequenceMatcher(None, a, b).ratio()


def similarity_narration(desc_1, desc_2):
	tokens_1 = re.split('[^a-zA-Z0-9]', desc_1)
	tokens_2 = re.split('[^a-zA-Z0-9]', desc_2)

	full_text_1 = ""
	full_text_2 = ""
	for each in tokens_1:
		full_text_1 += each
	for each in tokens_2:
		full_text_2 += each

	return similar(full_text_1, full_text_2)

# def get_unique_number(desc_1, desc_2):
# 	tokens_1 = re.split('[^a-zA-Z0-9]', desc_1)
#     tokens_2 = re.split('[^a-zA-Z0-9]', desc_2)
# 	commonalities = set(tokens_1) - (set(tokens_1) - set(tokens_2))
# 	final_list = list(commonalities)
# 	for each in final_list:
# 		if each.isdigit():
# 			digit += 1
#
#	return digit

def unique_number(desc_1, desc_2):
	tokens_1 = re.split('[^a-zA-Z0-9]', desc_1)
	tokens_2 = re.split('[^a-zA-Z0-9]', desc_2)

	common = set(tokens_1) - (set(tokens_1) - set(tokens_2))
	final_list = list(common)
	for each in final_list:
		if each.isdigit():
			digit += 1


	return digit




# ----------------------------------------------------
# Convert date to year-month string, This is useful to calculate each paramaters monthly
# Input: 01-03-2019, Output: "2018-03"
# ----------------------------------------------------

def date2yymm(dateobj):
	return str(dateobj.year) + "-" + (str(dateobj.month) if dateobj.month >= 10 else ('0' + str(dateobj.month)))


# ----------------------------------------------------
# Reject outliers based on sigma (m) method, assuming data is normally distributed
# m = 1: 68% (2 sigma), m = 2: 95% (4 sigma), m = 3: 99.7% (6 sigma)
# ----------------------------------------------------

def rejectOutliers(data, m = 1):	# Empirical Rule (Mean Based)
	try:
		data = np.array(data)
		mean = np.mean(data)
		spread = m * (np.std(data))
		idx = abs(data - np.mean(data)) <= m * np.std(data)
		return mean, mean - spread, mean + spread, idx, data[idx]
	except Exception as e:
		_, _, exc_tb = sys.exc_info()
		error_line = exc_tb.tb_lineno
		print "*ERROR: Reject Outliers :", str(e), error_line


# ----------------------------------------------------
# Checks whether if a pattern from a list fo pattern is present in a description
# A description is divided into 3 parts - 
# 1. Tokens (Remove all except alpha numeric and split in words)
# 2. Full Word (Remove all non alpha charatcters and concatenate)
# 3. Description (Original Description)
# If a mattern is match in any of the above then it is considered as a match
# ----------------------------------------------------

def elemInText(patternList, textKeywords, fullText, description, patternName = None):
	count, resPattern = 0, None

	if patternList == None or len(patternList) == 0 or textKeywords == None or len(textKeywords) == 0 or fullText == None or len(fullText) == 0:
		if patternName != None:
			return resPattern
		return count

	for p in range(len(patternList)):
		#  for each in
		if patternList[p] != None and patternList <> CORRECTED and ((len(patternList[p]) >= 2 and (patternList[p] in textKeywords)) or (not patternList[p].isalpha() and patternList[p] in description.lower())):
			count += 1
			if patternName != None:
				resPattern = patternName[p]
			break
		elif patternList == CORRECTED and patternList[p] in textKeywords[0]:
			count += 1
			if patternName != None:
				resPattern = patternName[p]
			break

	if patternName != None:
		return resPattern
	return count


# def elemInText(patternList, textKeywords, fullText, description, patternName = None):
# 	count, resPattern = 0, None
#
# 	if patternList == None or len(patternList) == 0 or textKeywords == None or len(textKeywords) == 0 or fullText == None or len(fullText) == 0:
# 		if patternName != None:
# 			return resPattern
# 		return count
#
# 	for p in range(len(patternList)):
# 		if patternList[p] != None and patternList <> CORRECTED:
# 			if (not patternList[p].isalpha() and patternList[p] in description):
# 				count += 1
# 				if patternName != None:
# 					resPattern = patternName[p]
# 				break
# 			for each in textKeywords:
# 				if (len(patternList[p]) >= 2 and (patternList[p] in each) and each[0:len(patternList[p])] == patternList[p]):
# 					count += 1
# 					if patternName != None:
# 						resPattern = patternName[p]
# 					break
#
# 		elif patternList[p] != None and patternList == CORRECTED and patternList[p] in textKeywords[0]:
# 			count += 1
# 			if patternName != None:
# 				resPattern = patternName[p]
# 			break
#
# 	if patternName != None:
# 		return resPattern
# 	return count

def elemInList(patternList, text):
	text, found = text.lower(), False
	for p in patternList:
		if (p.lower() in text and len(p) > 3) or (p.lower() in text.split()):
			found = True
	return found


# ----------------------------------------------------
# Get tokens and full words from given description as described in elemInText fuuntion
# Input - IMPS/P2A/flipkart/172893XPPA/SHUBHAM
# Output - Tokens - [IMPS, P2A, flipkart, XPPA, SHUBHAM], Full Word - IMPSP2AflipkartXPPASHUBHAM
# ----------------------------------------------------

def descriptionTokens(description):
	full_word, tokens = None, None
	if description != None and len(description) > 0:
		tokens = re.split('[^a-zA-Z0-9]', description)
		tokens = filter(None, tokens)
		full_word = re.sub(r'\W+', '', description).lower()
		for i, each in enumerate(tokens):
			tokens[i] = tokens[i].lower()
		for i in range (2,3):
			comb_list = list(itertools.permutations(tokens,i))
			for each in comb_list:
				word = ""
				for ele in each:
					word += ele
				tokens.append(word)

	return full_word, tokens


# ----------------------------------------------------
# Validate ordering of transaction based on Transaction Amount, Previous and Current Balance
# Previous Balance +/- Transaction Amount = Current Balance (Credit / Debit)
# This function also calculate number if instances where previous balance is in negative and current 
# transaction is of Debit type, as this tells a possibility of an OD Account
# ----------------------------------------------------

def validateTransactionsOrder(transaction_df):
	first_tran, prev_bal, prev_date, wrong_pair, neg_amt_allowed_tran, all_transaction_count = True, None, None, 0, 0, 0
	try:
		for index, row in transaction_df.iterrows():
			all_transaction_count += 1
			if first_tran:
				prev_date, prev_bal, first_tran = row['transactionDate'], row['balance'], False
				continue

			balance_difference = prev_bal + PAYMENT_CATEGORY[row['transactionType']] * row['transactionAmount'] - row['balance']

			if prev_bal < 0 and row['transactionType'] == 'debit' and abs(balance_difference) < 1 and row['transactionAmount'] >= MIN_CONSIDERABLE_CREDIT_AMOUNT: neg_amt_allowed_tran += 1
			if abs(balance_difference) >= 1.0: wrong_pair += 1

			prev_bal, prev_date = row['balance'], row['transactionDate']

	except Exception as e:
		_, _, exc_tb = sys.exc_info()
		error_line = exc_tb.tb_lineno
		print "*ERROR: validateTransactionsOrder", str(e), error_line

	return wrong_pair, neg_amt_allowed_tran, all_transaction_count



# ----------------------------------------------------
# This function calculate End-of-Day balance for each Day. If there is no transaction on a particular day, 
# Just previous EOD balance is considerd as EOD balance for that day 
# This is used to calculate the ABB(Average Bank Balance)
# ----------------------------------------------------

def calculateEODBalance(transaction_df):
	datewise_balance, datewise_balance2, prev_date, eod_balance, prev_eod_balance = [], [], None, [], 0

	try:
		for index, row in transaction_df.iterrows():
			if prev_date == None:
				prev_date, prev_eod_balance = row['transactionDate'], row['balance']
			else:
				if row['transactionDate'] != prev_date:
					datewise_balance.append((prev_date, prev_eod_balance))
				
				prev_date = row['transactionDate']
				prev_eod_balance = row['balance']
		
		datewise_balance.append((prev_date, prev_eod_balance))
		start_date, end_date = datewise_balance[0][0], datewise_balance[-1][0]
		delta = end_date - start_date
		idx = 0

		for t in range(delta.days + 1):
			tmp_date = start_date + timedelta(t)
			if not(tmp_date == datewise_balance[idx][0] or tmp_date < datewise_balance[idx+1][0]): idx = idx + 1	
			datewise_balance2.append((tmp_date, datewise_balance[idx][1]))

	except Exception as e:
		_, _, exc_tb = sys.exc_info()
		error_line = exc_tb.tb_lineno
		print "*ERROR: calculateEODBalance", str(e), error_line

	return datewise_balance2


# ----------------------------------------------------
# This function calculate Monthly Average Bank Balance (ABB) for given EOD balances over a transaction period. 
# This can be done in 3 ways - 
# 1. Average of all 30 days after removing outliers
# 2. Median of all 30 days
# 3. Median of 4 specific days (1, 5, 16, 25)
# ----------------------------------------------------

def calculateABB(datewise_balance):
	abb_sigma, abb_median_specific_days, abb_median_all_days = {}, {}, {}
	try:
		for d in datewise_balance:
			yyyy_mm = date2yymm(d[0])

			if yyyy_mm not in abb_sigma:
				abb_sigma[yyyy_mm], abb_median_specific_days[yyyy_mm], abb_median_all_days[yyyy_mm] = [], [], []
				
			abb_sigma[yyyy_mm].append(d[1])
			abb_median_all_days[yyyy_mm].append(d[1])
			if d[0].day in CREDIT_SPECIFIC_DATES:
				abb_median_specific_days[yyyy_mm].append(d[1])

		for k, v in abb_sigma.iteritems():
			_, _, _, _, abb_sigma[k] = rejectOutliers(v, m = 1.3)

			abb_sigma[k] = np.mean(np.array(abb_sigma[k]))
			abb_median_specific_days[k] = np.mean(np.array(abb_median_specific_days[k]))
			abb_median_all_days[k] = np.median(np.array(abb_median_all_days[k]))

	except Exception as e:
		_, _, exc_tb = sys.exc_info()
		error_line = exc_tb.tb_lineno
		print "*ERROR: calculateABB", str(e), error_line


	return abb_sigma, abb_median_specific_days, abb_median_all_days


# ----------------------------------------------------
# This function tag the appropriate categoro of a description. A category can be any vendor, lender,
# payment type or transaction type
# ----------------------------------------------------

def getDescriptionType(description, transactionId):
	res = {
		"transactionId": transactionId,
		"lender": 0,
		"lenderName": None,
		"vendor": 0,
		"vendorName": None,
		"otherLoan": 0,
		"charge": 0,
		"return": 0,
		"iwReturn": 0,
		"owReturn": 0,
		"cheque": 0,
		"cheque2": 0,
		"cash": 0,
		"neft": 0,
		"imps": 0,
		"rtgs": 0,
		"tpt": 0,
		"pos": 0,
		"wallet": 0,
		"nach": 0
	}

	try:
		description = description.lower()
		full_word, tokens = descriptionTokens(description)

		res["lender"] = elemInText(LENDER, tokens, full_word, description)
		res["lenderName"] = elemInText(LENDER, tokens, full_word, description, LENDER_NAME)
		res["vendor"] = elemInText(VENDOR, tokens, full_word, description)
		res["vendorName"] = elemInText(VENDOR, tokens, full_word, description, VENDOR_NAME)
		res["otherLoan"] = elemInText(OTHER_LOAN, tokens, full_word, description)
		res["charge"] = elemInText(CHARGES, tokens, full_word, description)
		res["return"] = elemInText(RETURN, tokens, full_word, description)
		res["iwReturn"] = elemInText(INWARD_RETURN, tokens, full_word, description)
		res["owReturn"] = elemInText(OUTWARD_RETURN, tokens, full_word, description)
		res["cheque"] = elemInText(CHEQUE, tokens, full_word, description)
		res["cash"] = elemInText(CASH, tokens, full_word, description)
		res["neft"] = elemInText(NEFT, tokens, full_word, description)
		res["imps"] = elemInText(IMPS, tokens, full_word, description)
		res["rtgs"] = elemInText(RTGS, tokens, full_word, description)
		res["tpt"] = elemInText(TPT, tokens, full_word, description)
		res["pos"] = elemInText(POS, tokens, full_word, description)
		res["wallet"] = elemInText(WALLET, tokens, full_word, description)
		res["nach"] = elemInText(NACH, tokens, full_word, description)
		res["internal"] = elemInText(INTERNAL, tokens, full_word, description)
		res['corr'] = elemInText(CORRECTED, tokens, full_word, description)

		if (res['neft'] + res['imps'] + res['rtgs'] + res['tpt'] + res['cash'] + res['pos'] + res['wallet'] ) == 0:
			res["cheque2"] = 1

	except Exception as e:
		_, _, exc_tb = sys.exc_info()
		error_line = exc_tb.tb_lineno
		print "*ERROR: getDescriptionType", str(e), error_line

	return res


# ----------------------------------------------------
# For every description this calls the above function (which tags the transaction) and merge it with 
# original dataframe
# ----------------------------------------------------

def tagTransactionType(transaction_df):
	description_column = transaction_df['description']
	transaction_id_column = transaction_df['transactionId']

	description_list = []
	for d, i in zip(description_column, transaction_id_column):
		description_list.append(getDescriptionType(d, i))

	description_df = pd.DataFrame(description_list)
	transaction_df = transaction_df.merge(description_df)
	return transaction_df


# ----------------------------------------------------
# As banking analysis consider past 6 months only for calculation, this function filter out all 
# other transaction
# ----------------------------------------------------

def filterTransactions(all_tranactions):
	try:
		all_tranactions = [list(elem) for elem in all_tranactions]
		all_tranactions = sorted(all_tranactions, reverse = True)
		all_tranactions2 = []

		last_transaction_date = all_tranactions[0][0]
		conisdered_year_months = set()

		for t in all_tranactions:
			yymm = date2yymm(t[0])

			if yymm in conisdered_year_months or dateDifference(t[0], last_transaction_date) <= 6*MONTH:
				all_tranactions2.append(t)
				conisdered_year_months.add(yymm)

		all_tranactions = list(reversed(all_tranactions2))
		return all_tranactions, list(conisdered_year_months)
	except Exception as e:
		_, _, exc_tb = sys.exc_info()
		error_line = exc_tb.tb_lineno
		print "*ERROR: filterTransactions", str(e), error_line
		return None, None


# ----------------------------------------------------
# As banking analysis consider past 6 months only for calculation, this function filter out all 
# other transaction
# ----------------------------------------------------

def processAllTransactions(all_tranactions, columns):
	try:
		types = {
			'transactionAmount': 'float64',
			'balance': 'float64'
		}
		all_tranactions, conisdered_year_months = filterTransactions(all_tranactions)
		transaction_df = pd.DataFrame(all_tranactions, columns = columns)
		transaction_df = transaction_df.astype({'transactionAmount': 'float64', 'balance': 'float64'})
		transaction_df = tagTransactionType(transaction_df)
		conisdered_year_months = sorted(conisdered_year_months, reverse = True)
		return transaction_df, conisdered_year_months
	except Exception as e:
		_, _, exc_tb = sys.exc_info()
		error_line = exc_tb.tb_lineno
		print "*ERROR: processAllTransactions", str(e), error_line
		return None, None


# ----------------------------------------------------
# Get correspond all transactions from DB for a particular bank account transaction ID
# ----------------------------------------------------

def getAllTransactions(bank_transaction_id, cursor):
	try:
		query = "select date(transaction_date) as transactionDate, id as transactionId, cheque_no as chequeNo, description as description, lower(category) as transactionCategory, amount as transactionAmount, lower(transaction_type) as transactionType, balance as balance from bank_db.bank_account_transactions where transaction_type is not NULL and  bank_account_summary_id = '%s'" %  bank_transaction_id
		cursor.execute(query)
		columns = cursor.description
		columns = [list(elem)[0] for elem in columns]
		all_tranactions = cursor.fetchall()

		return all_tranactions, columns
	except Exception as e:
		_, _, exc_tb = sys.exc_info()
		error_line = exc_tb.tb_lineno
		print "*ERROR: getAllTransactions", str(e), error_line
		return None, None