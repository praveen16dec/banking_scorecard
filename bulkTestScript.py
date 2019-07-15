import bankingScorecardModel as bsm
import bankingScorecardUtils as bsu

import json
import pandas as pd
import sys
import time

all_parsed_banking_cases = pd.read_csv("/Users/praveenyadav/Documents/workstation/DS_Banking/poas_aditi_1207_input.csv")


done = 0
stored = 0



input_data = {}
for data in all_parsed_banking_cases.iterrows():
    if 'Savings' not in bsu.account_type(data[1][1])[0] or 'OD' not in bsu.account_type(data[1][1])[0]:
        if data[1][0] not in input_data:
            input_data[data[1][0]] = {"id" : []}

            input_data[data[1][0]]['id'].append(data[1][1])
        else:
            input_data[data[1][0]]['id'].append(data[1][1])


#input_data = {"5cbec5f3rtw1q" : {"id" : [38751], "credit_status" : "rejected", "created_at" : "2019-02-01"}}

final_rejection_result = []
for key, val in input_data.items():
    final_result = {
        'success': True,
        'message': 'ok',
        'result': None,
        'bank_transaction_id': val['id']
    }
    try:
        loan_code = key
        final_summary_result = []
        start_time = time.time()
        all_abb = []
        for id in val['id']:
            account_summary_id = id
            res_summary, vendor_credit_sale = bsm.startApplication(account_summary_id, loan_code)

            if res_summary != None:
                if 'total_transaction_period' in res_summary and res_summary['total_transaction_period'] < 150:
                    final_result['success'] = False
                    final_result['message'] = 'less than 150 days transactions'
                else:
                    final_summary_result.append(res_summary)
                    all_abb.append(res_summary["monthly_abb"])#
            else:
                final_result['success'] = False
                final_result['message'] = 'Unable to extract information from banking data'


        is_od = False
        for each in all_abb:
            count_neg = 0
            for ele in each:
                if ele < 0:
                    count_neg += 1
            if count_neg >= 3:
                is_od = True

        new_final_result = {}
        if len(final_summary_result) != 0:
            if None not in final_summary_result:
                summary_data_collection, feature_data_collection, feature_vector_collection = bsm.summary_to_feature(final_summary_result)
                for in_key, in_val in feature_vector_collection.items():
                    new_final_result[in_key] = {"success" : True, "message" : "ok", "result" : {}}
                    score, confidence, predictions, feature_contribution = bsm.predictResult(in_val)

                    new_final_result[in_key]['result'] = {
                        'score': score,
                        'confidence': confidence,
                        'predictions': predictions,
                        'feature_contribution': feature_contribution,
                        'summary_data': summary_data_collection[in_key],
                        # 'vendor_sales': vendor_credit_sale,
                        'feature_data': feature_data_collection[in_key]
                    }
                    print new_final_result[in_key]['result']["feature_contribution"]
                    feat = new_final_result[in_key]['result']["feature_contribution"]
                    summ = new_final_result[in_key]['result']["summary_data"]
                    #print new_final_result
                    print new_final_result[in_key]['result']['summary_data']['f8_iw_count'], new_final_result[in_key]['result']['summary_data']['f8_iw_bounce']
                    print new_final_result[in_key]['result']['summary_data']['f9_ow_count'], new_final_result[in_key]['result']['summary_data']['f9_ow_bounce']

                    final_rejection_result.append(
                        {"loan_code": loan_code,
                         "bank_summary_id" : val["id"],
                         "bank_score" : new_final_result[in_key]['result']['score'],
                         "score_confidence" : new_final_result[in_key]['result']['confidence'],
                         "business_transactions" : new_final_result[in_key]['result']["summary_data"]["f2_credit_instance"],
                         "is_od_present" : is_od,
                         "emi_bounce" : new_final_result[in_key]['result']['summary_data']['f6_emi_bounces_keywords'],
                         "inward_count": new_final_result[in_key]['result']['summary_data']['f8_iw_count'],
                         "inward_return": new_final_result[in_key]['result']['summary_data']['f8_iw_bounce'],
                         "inward_return_ratio": new_final_result[in_key]['result']['summary_data']['inward_ratio'],
                         "outward_count": new_final_result[in_key]['result']['summary_data']['f9_ow_count'],
                         "outward_return": new_final_result[in_key]['result']['summary_data']['f9_ow_bounce'],
                         "outward_return_ratio": new_final_result[in_key]['result']['summary_data']['outward_ratio'],
                         "monthly_bto": new_final_result[in_key]['result']['summary_data']['monthly_credit_sales'],
                         "monthly_abb": new_final_result[in_key]['result']['summary_data']['monthly_abb'],
                         "total_cash_sale" : new_final_result[in_key]['result']['summary_data']['total_cash_sale'],
                         "feature_contribution" : feat})
            else:
                new_final_result['success'] = False
                new_final_result['message'] = 'Unable to extract information from banking data'
                #final_rejection_result.append({"loan_code": loan_code, "inward_count" : 0, "inward_return" : 0, "outward_count" : 0, "outward_return" : 0})

            #print final_result
        end_time = time.time()


        process_time = end_time - start_time
        print process_time
        version = '0.1.0'

        # if final_result != None and 'success' in final_result and final_result['success'] and 'result' in final_result and final_result['result'] != None:
        #     final_result['result']['processTime'] = process_time
        #     final_result['result']['version'] = version
        #     final_result['loan_code'] = loan_code
        #
        #     score = None
        #     if 'score' in final_result['result'] and final_result['result']['score'] != None:
        #         score = final_result['result']['score']
        #
        #         db, cursor = bsu.getDBInstance()
        #
        #         loan_code = final_result['loan_code']
        #         bank_account_summary_id = final_result['bank_transaction_id']
        #         score = score
        #         score_data = json.dumps(final_result['result'])
        #
        #         try:
        #             cursor.execute(
        #                 """INSERT INTO bank_db.banking_scorecard (loan_code, bank_account_summary_id, score, score_data) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE  score = %s, score_data = %s""",
        #                 (loan_code, bank_account_summary_id, score, score_data, score, score_data))
        #             db.commit()
        #         except Exception as e:
        #             print "Exception error", str(e)
        #             pass
        #
        #         stored += 1
        #         db.close()
        #
        #     else:
        #         print "Some Error"

    except Exception as e:
        _, _, exc_tb = sys.exc_info()
        error_line = exc_tb.tb_lineno
        print "*Final Error", str(e), error_line

    done += 1
    print done

print final_rejection_result

#             if res != None and 'success' in res and res['success'] and 'result' in res and res['result'] != None:
#                 res['result']['processTime'] = process_time
#                 res['result']['version'] = version
#                 res['loan_code'] = loan_code
#
#                 score = None
#                 if 'score' in res['result'] and res['result']['score'] != None:
#                     score = res['result']['score']
#
#                     # #db, cursor = bsu.getDBInstance()
#                     #
#                     # loan_code = res['loan_code']
#                     # bank_account_summary_id = res['bank_transaction_id']
#                     # score_data_1 = (res['result']['summary_data']['monthly_credit_sales'])
#                     # score_data_2 = (res['result']['summary_data']['monthly_abb'])
#                     # inw_count = res['result']['summary_data']['f8_iw_count']
#                     # otw_count = res['result']['summary_data']['f9_ow_count']
#                     # inw_ret = res['result']['summary_data']['f8_iw_bounce']
#                     # otw_ret = res['result']['summary_data']['f9_ow_bounce']
#                     # abb_trend = res['result']['summary_data']['f13_abb_trend']
#                     #
#                     # try:
#                     #     bto_trend = score_data_1[0] - score_data_1[2]
#                     #
#                     #     if bto_trend > 0: lqbt = "increasing"
#                     #     elif bto_trend < 0: lqbt = "decreasing"
#                     #     else: lqbt = "constant"
#                     # except ZeroDivisionError:
#                     #     lqbt = "increasing"
#                     #
#                     # try:
#                     #     abb_trend = score_data_2[0] - score_data_2[2]
#                     #     if abb_trend > 0: lqat = "increasing"
#                     #     elif abb_trend < 0: lqat = "decreasing"
#                     #     else: lqat = "constant"
#                     # except ZeroDivisionError:
#                     #     lqat = "increasing"
#                     #
#                     # qtr_1_bto = score_data_1[3:6]
#                     # qtr_1_bto_avg = mean(qtr_1_bto)
#                     # qtr_2_bto = score_data_1[0:3]
#                     # qtr_2_bto_avg = mean(qtr_2_bto)
#                     # try:
#                     #     qtr_1_bto_sum = sum(qtr_1_bto)
#                     #     qtr_2_bto_sum = sum(qtr_2_bto)
#                     #     decline_bto = qtr_2_bto_sum * 1.0 / qtr_1_bto_sum
#                     #
#                     #     decline_1 = score_data_1[1] * 1.0 / score_data_1[2]
#                     #     decline_2 = score_data_1[0] * 1.0 / score_data_1[1]
#                     #
#                     #     decline_new = (decline_1 + decline_2) * 1.0 / 2
#                     #
#                     #     if decline_new < 0.2: decline_bto_lq = 1
#                     #     else : decline_bto_lq = 0
#                     #
#                     #     if decline_bto < 0.5 : dec_bto = 1
#                     #     else: dec_bto = 0
#                     # except ZeroDivisionError:
#                     #     dec_bto = 1
#                     #     decline_bto_lq = 1
#                     #
#                     #
#                     # qtr_1_abb = score_data_2[3:6]
#                     # qtr_1_abb_avg = mean(qtr_1_abb)
#                     # qtr_2_abb = score_data_2[0:3]
#                     # qtr_2_abb_avg = mean(qtr_2_abb)
#                     # try:
#                     #     qtr_1_abb_sum = sum(qtr_1_abb)
#                     #     qtr_2_abb_sum = sum(qtr_2_abb)
#                     #     decline_abb = qtr_2_abb_sum * 1.0 / qtr_1_abb_sum
#                     #
#                     #     if decline_abb < 0.5 : dec_abb = 1
#                     #     else: dec_abb = 0
#                     # except ZeroDivisionError: dec_abb = 1
#                     #
#                     # if dec_abb == 0 and decline_bto_lq == 1:
#                     #     abb_not_bto = "abb decline but bto good"
#                     # elif dec_abb == 0 and decline_bto_lq == 0:
#                     #     abb_not_bto = "abb decline and bto decline bad"
#                     # else: abb_not_bto = "no abb decline"
#                     #
#                     #
#                     # final_result.append({"loan_code" : loan_code, "bank_account_summary_id" : account_summary_id, "application_number" : app_num, "reason" : reason,
#                     #                      "bto_1" : score_data_1[0], "bto_2" : score_data_1[1], "bto_3" : score_data_1[2], "bto_4" : score_data_1[3],
#                     #                      "bto_5" : score_data_1[4], "bto_6" : score_data_1[5], "abb_1" : score_data_2[0], "abb_2" : score_data_2[1],
#                     #                      "abb_3" : score_data_2[2], "abb_4" : score_data_2[3], "abb_5" : score_data_2[4], "abb_6" : score_data_2[5],
#                     #                      "is_bto_decline" : dec_bto, "is_abb_decline" : dec_abb, "latest_quarter_bto_trend" : lqbt, "latest_quarter_abb_trend" : lqat,
#                     #                      "abb_vs_bto" : abb_not_bto, "q1_bto_avg" : qtr_1_bto_avg, "q2_bto_avg" : qtr_2_bto_avg, "q1_abb_avg" : qtr_1_abb_avg,
#                     #                      "q2_abb_avg" : qtr_2_abb_avg, "abb_trend" : abb_trend})
#
#                     # final_result.append({"loan_code": loan_code, "bank_account_summary_id": account_summary_id, "UID": uid,
#                     #                      "bto_1": score_data_1[0], "bto_2": score_data_1[1], "bto_3": score_data_1[2], "bto_4": score_data_1[3],
#                     #                      "bto_5": score_data_1[4], "bto_6": score_data_1[5], "abb_1": score_data_2[0], "abb_2": score_data_2[1],
#                     #                      "abb_3": score_data_2[2], "abb_4": score_data_2[3], "abb_5": score_data_2[4], "abb_6": score_data_2[5],
#                     #                      "q1_bto_avg": qtr_1_bto_avg, "q2_bto_avg": qtr_2_bto_avg, "q1_abb_avg": qtr_1_abb_avg, "q2_abb_avg": qtr_2_abb_avg,
#                     #                      "credit_transactions" : otw_count, "debit_transactions" : inw_count, "inward_bounce_count" : inw_ret,
#                     #                      "outward_bounce_count" : otw_ret})
#                     # try:
#                     #     cursor.execute("""INSERT INTO bank_db.banking_scorecard (loan_code, bank_account_summary_id, score, score_data) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE  score = %s, score_data = %s""",  (loan_code, bank_account_summary_id, score, score_data, score, score_data))
#                     #     db.commit()
#                     # except Exception as e:
#                     #     print "Exception error", str(e)
#                     #     pass
#
#                     stored += 1
#                     #db.close()
#
#                 else:
#                     print "Some Error"
#
#         except Exception as e:
#             print "*Final Error", str(e)
#
#         done += 1
#         print done, stored
#
# print final_result
#
#
#
# # account_summary_id = "31783" #row['id']
# # loan_code = "5c1720c408571" #row['loan_code']
# # start_time = time.time()
# #
# # # if account_summary_id in already_done:
# # #     continue
# #
# # res = bsm.startApplication(account_summary_id, loan_code)
# # end_time = time.time()
# #
# # process_time = end_time - start_time
# # version = '0.1.0'
# #
# # if res != None and 'success' in res and res['success'] and 'result' in res and res['result'] != None:
# #     res['result']['processTime'] = process_time
# #     res['result']['version'] = version
# #     res['loan_code'] = loan_code
# #
# #     score = None
# #     if 'score' in res['result'] and res['result']['score'] != None:
# #         score = res['result']['score']
# #
# #         # db, cursor = bsu.getDBInstance()
# #
# #         loan_code = res['loan_code']
# #         bank_account_summary_id = res['bank_transaction_id']
# #         score = score
# #         score_data_1 = json.dumps(res['result'])
# #         #score_data_2 = json.dumps(res['result']['summary_data']['monthly_abb'])
# #
# #         print score_data_1
# #     else:
# #         print "Some Error"


