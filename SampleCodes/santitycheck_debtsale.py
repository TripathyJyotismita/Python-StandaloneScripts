import os
import sys
import MySQLdb
import csv
import traceback
from decimal import Decimal
conn = MySQLdb.connect('localhost','opsuser','OPS!@#','TMS_Data')
curs = conn.cursor()
csv_read = csv.reader(open("/home/sheik.h/Debt_sale_20131022.csv"), delimiter='\t')
csv_ok = csv.writer(open("/home/sheik.h/Debtsell/Debt_sale_20131009_ok.csv","wb"), quoting=csv.QUOTE_ALL, delimiter='\t')
csv_remove = csv.writer(open("/home/sheik.h/Debtsell/Debt_sale_20131009_remove.csv","wb"), quoting=csv.QUOTE_ALL, delimiter='\t')

tworound = 0
ltround = 0
checkround = 0
checkloanid = []
loan_id = []
dca = []
ncpe = []
status = []
sgtedate = []
soldpyt = []
opb_list = []
SUM = 0
account = 0

csv_read.next()
for row in csv_read:
	account = account + 1
	placequery = "select count(*) from TMS_Data.Collection_Info where loan_id = '%s'" % str(row[0])
#	print placequery
	curs.execute(placequery)
	count = curs.fetchone()
#	print count[0]
	count_list = list(count)
	if count_list[0] == 2:
		tworound = tworound + 1
		loan_id.append(row[0])
	elif count_list[0] > 2:
		ltround = ltround + 1
		checkloanid.append(row[0])
	else:
		checkround = checkround + 1
		checkloanid.append(row[0])
	ndca = "select loan_id, dca_name from TMS_Data.Collection_Info where end_dt is NULL and loan_id = '%s'" % str(row[0])
#	print ndca
	curs.execute(ndca)
	ndcachk = curs.fetchall()
	ndcachk_list = list(ndcachk)	
	if ndcachk_list:
		dca.append(ndcachk_list)
	cpecheck = "select loan_id, dca_name,closure_cd from TMS_Data.Collection_Info where closure_cd!='CPE' and loan_id = '%s'" % str(row[0])
#	print cpecheck
	curs.execute(cpecheck)
	cpechk = curs.fetchall()
	cpechk_list = list(cpechk)	
	if cpechk_list:
		ncpe.append(row[0])
	statuscheck = "select entity_id,status_cd from GCMS_Data.gcm_case where entity_type='LOAN' and status_cd!='RETURNED' and entity_id = '%s'" % str(row[0])
#	print statuscheck
	curs.execute(statuscheck)
	statuschk = curs.fetchall()
	statuschk_list = list(statuschk)
	if statuschk_list:
		status.append(statuschk_list)
	sgecheck ="select loan_id from TMS_Data.Collection_Info where date(start_dt) > date(end_dt) and loan_id= '%s'" % str(row[0])
#	print sgecheck
	curs.execute(sgecheck)
	sgechk = curs.fetchall()
	sgechk_list = list(sgechk)
	if sgechk_list:
		sgtedate.append(sgechk_list)
	sold3pyt = "select loan_id,tran_dt,debit,payment_method from TMS_Data.Transactions where date(tran_dt)>date(now() - Interval 3 month) and loan_id = '%s'" % str(row[0])
#	print sold3pyt
	curs.execute(sold3pyt)
	sold3pyt_dt = curs.fetchall()
	sold3pyt_dtlist = list(sold3pyt_dt)
	if sold3pyt_dtlist:
		soldpyt.append(sold3pyt_dtlist)
	OPB = "select loan_id,OPB,due_amt from TMS_Data.Payments where OPB <= 0 and loan_id = '%s'" % str(row[0])
	curs.execute(OPB)
	OPB_dt = curs.fetchall()
	OPB_dtlist = list(OPB_dt)
	if OPB_dtlist:
		opb_list.append(OPB_dtlist)
	SUM = SUM+Decimal(row[74])
print "\nNo. of Accounts not passed two rounds of collection '%s'" % str(checkround)
print "\nAccounts still open with DCA %s" % str(dca)
print "\nAccounts not closed with CPE %s"% str(ncpe)
print "\nAccounts not in RETURNED status %s" % str(status)
print "\nAccounts with start date greater than end date %s" % str(sgtedate)
print "\nPayments received in Last 3 Months %s" % str(soldpyt)
print "\nOutStanding Principle less than and equal to zero %s\n" % str(opb_list)
print "\nTotal sum of Amount %s \n" % str(SUM)
print "\nNumber of Account in the file %s \n" % str(account)
