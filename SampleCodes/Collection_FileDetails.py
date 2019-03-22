import os
import sys
import datetime
import time
import traceback
import csv
import CollectionsftpUP as CC
from decimal import Decimal
os.environ['PYTHON_EGG_CACHE'] = '/home/sheik.h/Reports'
import MySQLdb
conn = MySQLdb.connect('localhost','opsuser','OPS!@#','ukl_collections')
curs = conn.cursor()
curdate=datetime.datetime.now()

def validate(date_text):
	try:
		valid_date = time.strptime(date_text, '%Y-%m-%d')
	except ValueError:
		print('Invalid date!')
		sys.exit()


if __name__=='__main__':
	username = raw_input("Enter your name:")
	while True:
		if username == 'sheik.h':
			opt = raw_input("1. Payment to be updated\n2. Closure to be updated\n3. Approve Payments\nEnter from the above option:")
		elif:
			opt = raw_input("1. Payment to be updated\n2. Closure to be updated\nEnter from the above option:")
		agency = raw_input("1. MH\n2. CRS\nLoan ID associated with the agency:")
		while agency != '1' and agency != '2':
			agency = raw_input("Enter the valid Agency option")
		if agency == '1':
			agency_name = 'MH' 
		elif agency == '2':
			agency_name = 'CRS'
#		if opt == '3':
#			select 
		AgreeNbr = raw_input("Enter the Agreement Number:")
		statusquery = "select status_cd from GCMS_Data.gcm_case where entity_type='LOAN' and entity_id = '%s'" % str(AgreeNbr)
		curs.execute(statusquery)
		sq = curs.fetchall()
		print "Status is %s" % str(sq)
		dca_query = "select placement,dca_name,start_dt,end_dt,closure_cd,override_flag from TMS_Data.Collection_Info where loan_id = %s" % str(AgreeNbr)
		curs.execute(dca_query)
		dq = curs.fetchall()
		print dq
		if opt == '1':
			check_query = "select * from ukl_collections.FileUpdate_Payment where loan_id = %s" % str(AgreeNbr)
			curs.execute(check_query)
			cq = curs.fetchall()
			if cq:
				print cq
				confirm = raw_input("\nStill do you need to proceed (Y or N):").upper()
				if confirm != 'Y':
					sys.exit()
			InvBalQuery = "select (pc.OutstandingInterest+pc.OutstandingFee+py.OPB) IB from TMS_Data.Payments py join (select loan_id,sum(if(pc.payment_type rlike 'LZD',pc.payment_amt-pc.paid_amt,0)) as OutstandingInterest,sum(if(pc.payment_type rlike 'FEE',pc.payment_amt-pc.paid_amt,0)) as OutstandingFee from TMS_Data.PaymentCalendar as pc where loan_id in ('%s') and override_flag=0 group by loan_id) pc on py.loan_id = pc.loan_id" % (str(AgreeNbr))
			curs.execute(InvBalQuery)
			IBV = curs.fetchone()
			IBV_list=list(IBV)
			print "Outstanding Balance is %s" % (str(IBV_list[0]))
			Amt = raw_input("Enter the Amount:")
			tran_dt = raw_input("Enter the Transaction Date(YYYY-MM-DD):")
			validate(tran_dt)
			tran_id = raw_input("Enter the Transaction ID:")
			tran_type_opt = raw_input("1. Debit\t2. Waived\t3. Refund\n\nEnter the transaction type:")
			if tran_type_opt > '3':
				tran_type_opt = raw_input("Enter valid input\n1. Debit\t2. Waived\t3. Refund\n\nEnter the transaction type:")
			if tran_type_opt == '1':
				tran_type = "Debit"
				pbalance = IBV_list[0] - Decimal(Amt)
				print "Projected Balance After adding of this amount %s" % (str(pbalance))
			elif tran_type_opt == '2':
				tran_type = "Waived"
				pbalance = IBV_list[0] - Decimal(Amt)
				print "Projected Balance After waiving of this amount %s" % (str(pbalance))
			elif tran_type_opt == '3':
				tran_type = "Refund"
				pbalance = IBV_list[0] + Decimal(Amt)
				print "Projected Balance After refunding of this amount %s" % (str(pbalance))
			else:
				print "Enter the valid option"
				sys.exit()
			DCtrails4 = raw_input("Enter the DCtrails4:")
			opcode = raw_input("Enter the op-code:")
			values = (str(AgreeNbr),str(agency_name),str(tran_id),str(tran_dt),str(Amt),str(tran_type),str(DCtrails4),str(opcode),str(curdate),str(username),'0')
			print values
			ins_query = 'insert into ukl_collections.FileUpdate_Payment values %s' % str(values)
			print ins_query
			curs.execute(ins_query)
		elif opt == '2':
			closure_dt = raw_input("\nEnter the Closure Date(YYYY-MM-DD):")
			validate(closure_dt)
#			closure_cd = raw_input("1.CLI_REQ\n2.DUP\n3.FRAUD\n4.DISP\n5.SIF\n6.CPE\n7.PIF\n8.IVA\n9.UNC\n10.PRISON\n11.RTC\n12.UEP\n13.DRO\n14.UKN\n15.TEX\n16.CRQ\n17.SET\n18.BKT\n19.FRD\n20.HDS\n21.DEC\n22.DECEASED\n23.PTP\nEnter the exact Closure Code:").upper()
			closure_cd = raw_input("CLI_REQ\nDUP\nFRAUD\nDISP\nSIF\nCPE\nPIF\nIVA\nUNC\nPRISON\nRTC\nUEP\nDRO\nUKN\nTEX\nCRQ\nSET\nBKT\nFRD\nHDS\nDEC\nDECEASED\nPTP\nEnter the exact Closure Code:").upper()
			Agent = raw_input("Enter the agent name:")
			values = (str(AgreeNbr),str(closure_dt),str(closure_cd),str(Agent),str(agency_name),str(curdate),str(username))
			print values
			ins_query = 'insert into ukl_collections.FileUpdate_Closure values %s' % str(values)
			curs.execute(ins_query)
#			print "Closure report is under construction, so do this manually"
	   	fcheck = raw_input("Do you need to continue (Y or N):").upper()
		if fcheck != 'Y':
			sys.exit()
