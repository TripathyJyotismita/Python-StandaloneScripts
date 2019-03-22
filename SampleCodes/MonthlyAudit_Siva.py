import os
os.environ['PYTHON_EGG_CACHE'] = os.getcwd()
os.sys.path.append("/public/collections/codes")
import MySQLdb
import csv
import datetime
import sys
import xlrd
conn=MySQLdb.connect('localhost','opsuser','OPS!@#','ukl_collections')
curs =conn.cursor()
curr=conn.cursor()
ins=conn.cursor()
dtnow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
today = datetime.datetime.now().strftime("%Y-%m-%d")


def updatecrs(file):
	csv_read = csv.reader(open(file,'rb'),delimiter = ',',quotechar = '"')
	for row in csv_read:
	        print row
#        	for j in i:
#              	row=j.split(',')
	#       print row[0],s[1],s[2],
		row = [ i.replace("\xc2\xa3","") for i in row]
       	        values = (str(row[0]),str(row[1]),str(row[2]),str(row[3]),str(row[4]),str(row[5]),str(row[6]),str(row[7]),str(row[8]),str(row[9]),dtnow)
               	print values
                inst_values = 'insert into ukl_collections.CRS_MonthlyInvoice values %s' % str(values)
		print inst_values
                ins.execute(inst_values)
	print "CRS billing Report updated successfully"


def updatemh(file):
	csv_read = csv.reader(open(file,'rb'),delimiter = ',',quotechar = '"')
	for row in csv_read:
		print row
		row = [ i.replace("\xc2\xa3","") for i in row]
		values = (str(row[0]),str(row[1]),str(row[2]),str(row[3]),str(row[4]),str(row[5]),str(row[6]),str(row[7]),str(row[8]),str(row[9]),str(row[10]),str(row[11]),dtnow)
		Insertquery="insert into ukl_collections.MHMonthlyInvoice values %s" % str(values)
		print Insertquery
		ins.execute(Insertquery)
	print "MH billing Report updated successfully"

'''def updatemh(file):
	s=file
	print  'this is vlaue for xlsx' ,s
	book=xlrd.open_workbook(s)
	sheet=book.sheet_by_index(0)
	print sheet.nrows
	for row_index in range(1,sheet.nrows):
		row_num=row_index
		Client_Name=sheet.cell(row_index,1).value
                Client=sheet.cell(row_index,2).value
                MHAccountID=sheet.cell(row_index,3).value
                ClientID=sheet.cell(row_index,4).value
		Customer_Name=sheet.cell(row_index,5).value
#		x_Date=datetime.datetime(*xlrd.xldate_as_tuple(x_Date_value), book.datemode)).date()
		TranID=sheet.cell(row_index,6).value
		Net_Collected = sheet.cell(row_index,7).value
		Cleared_Date = sheet.cell(row_index,8).value
		Collected_Date = sheet.cell(row_index,9).value
#		y_Date=datetime.datetime(*xlrd.xldate_as_tuple(y_Date_value, book.datemode)).date()
		Commission=sheet.cell(row_index,10).value
		Amount_Comm=sheet.cell(row_index,11).value
		Remaining_Amount =sheet.cell(row_index,12).value
		
#		Insertquery="insert into ukl_collections.MHMonthlyInvoice values('%s','%s','%s','%s','%s','%s','%s','%s')"%(LoanID,AscentRef,CustName,amt,x_Date,y_Date,x_amt,y_amt)
#		Insertquery="insert into ukl_collections.MHMonthlyInvoice values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(Client_Name,Client,MHAccountID,ClientID,Customer_Name,TranID,Net_Collected,Cleared_Date,Collected_Date,Commission,Amount_Comm,Remaining_Amount)
		Insertquery= "insert into ukl_collections.MHInvoiceMonthly values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s') " %(Client_Name,Segment,MHAccountID,ClientID,Customer_Name,TranID,Net_Collected,Collected_Date,Cleared_Date,Commission,Amount_Comm)
		print Insertquery'''

def crsprocess():
	yr = raw_input("Enter the year you want to compare YYYY:")
	month = raw_input("Enter the month you want to compare MM:")
	Agr_list_query = 'select distinct Client_Ref from ukl_collections.CRS_MonthlyInvoice where date(create_dt)=curdate()'
	Agr_list_e = curs.execute(Agr_list_query)
	Agr_list =curs.fetchall()
	Agr_Lt = list(Agr_list)
	for u in Agr_Lt:
		values=()
#	h=str(u[0])
		AgrNbr = str(u[0])
	#       values.append(str(h))
	#	values.append(str(dt))
		sum_query="select sum(Amt_Paid_CRS) crs_paid,Current_Balance from ukl_collections.CRS_MonthlyInvoice where date(create_dt)='"+today+"' and Client_Ref='"+AgrNbr+"';"
		print sum_query
		sum_detail = curs.execute(sum_query)
		sum = curs.fetchall()
		sum_list = list (sum)
		tms_sum_query="select (sum(debit)-sum(credit)) tms_paid from  (select tran_dt ,debit,credit,loan_id from TMS_Data.Transactions where  loan_id='"+AgrNbr+"' and year(tran_dt)='"+yr+"' and month(tran_dt)='"+month+"'and  payment_method='COLLECTION AGENCY' and merchant_name='CRS') y;"
		print tms_sum_query
		tms_sum_e = curr.execute(tms_sum_query)
		tms_sum_dt = curr.fetchall()
		tms_sum_list = list(tms_sum_dt)
	#	values=(str(h),str(dt),str(v),str(w))
	#	inst_values = 'insert into ukl_collections.crs_bill_ssn_logic values %s' % str(values)
		inv="select py.loan_id,(pc.OutstandingInterest+pc.OutstandingFee+py.OPB) as InventoryBalance,pc.OutstandingInterest,pc.OutstandingFee,py.OPB from TMS_Data.Payments py join (select loan_id,sum(if(pc.payment_type rlike 'LZD',pc.payment_amt-pc.paid_amt,0)) as OutstandingInterest,sum(if(pc.payment_type rlike 'FEE',pc.payment_amt-pc.paid_amt,0)) as OutstandingFee from TMS_Data.PaymentCalendar as pc where loan_id in ('"+AgrNbr+"') and override_flag=0 group by loan_id) pc on py.loan_id = pc.loan_id"
		ii=curr.execute(inv)
		io=curr.fetchall()
		for e in io:
			iv_bal=str(e[1])
			opb=str(e[4])
		sta="select status_cd,closure_cd,end_dt from GCMS_Data.gcm_case join TMS_Data.Collection_Info on loan_id=entity_id where dca_name='CRS' and              case_type_cd='LOAN' and  loan_id='"+AgrNbr+"';"
		print sta
		st=curr.execute(sta)
	        sto=curr.fetchall()
	#	print sto
		for cc in sto:
			ts=str(cc[0])
			cs=str(cc[1])
			ddt=str(cc[2])
	#		print ts,cs
		dt = yr+'-'+month
		values=(str(AgrNbr),str(dt),str(sum_list[0][0]),str(tms_sum_list[0][0]),str(str(sum_list[0][1])),str(iv_bal),str(opb),str(ts),str(cs),str(ddt))		
		print values
		inst_values = "insert into ukl_collections.crs_bill_ssnlogic_new values %s" % str(values)
		ins.execute(inst_values)
	print "crs finished"

def mhprocess():
	opt = int(raw_input("Do you like to compare this invoice with \n 1. Date Interval \n 2. Transactions ID"))
	if opt == 1:
		dt='201402'
		yr = dt[:-2]
		month = int(dt[-2:])
		startdate = yr+'-'+str(month)+'-06'
		month_1 = month+1
		enddate = yr+'-'+str(month_1)+'-06'
	        loan_list='select distinct ClientID from ukl_collections.MHMonthlyInvoice'
        	st=curs.execute(loan_list)
	        all_loan = curs.fetchall()
        	for l in all_loan:
                	values=()
	                lid=str(l[0])
#       	        mh_amt ="select sum(Net_Collected) mh_paid from ukl_collections.MHMonthlyInvoice where year(Cleared_Date)='"+yr+"' and month(Cleared_Date)>='"+month+"' and ClientID='"+str(lid)+"';"
			mh_amt ="select sum(Net_Collected) mh_paid from ukl_collections.MHMonthlyInvoice where date(Cleared_Date) between '"+startdate+"' and '"+enddate+"' and ClientID='"+str(lid)+"';"
	                tran_sum = curs.execute(mh_amt)
        	        tran_sumdetail = curs.fetchall()
			MH_transum = str(tran_sumdetail[0][0])
	#                tms="select (sum(debit)-sum(credit)) tms_paid from  (select tran_dt ,debit,credit,loan_id from TMS_Data.Transactions where  loan_id='"+str(h)+"' and replace(substr(tran_dt,1,7),'-','')='"+dt+"' and  payment_method='COLLECTION AGENCY' and merchant_name='Mackenzie Hall') y;"
#			tms = "select (sum(debit)-sum(credit)) tms_paid from TMS_Data.Transactions where  loan_id='"+lid+"' and year(tran_dt) = '"+yr+"' and month(tran_dt)>='"+month+"' and payment_method='COLLECTION AGENCY' and merchant_name='Mackenzie Hall'"
			tms = "select (sum(debit)-sum(credit)) tms_paid from TMS_Data.Transactions where  loan_id='"+lid+"' and date(tran_dt) between '"+startdate+"' and '"+enddate+"' and payment_method='COLLECTION AGENCY' and merchant_name='Mackenzie Hall'"
	                tms_tran_sum = curr.execute(tms)
        	        tms_tran_sumdetail = curr.fetchall()
			TMS_Balsum = str(tms_tran_sumdetail[0][0])
	                inv_bal_q = "select py.loan_id,(pc.OutstandingInterest+pc.OutstandingFee+py.OPB) as InventoryBalance,pc.OutstandingInterest,pc.OutstandingFee,py.OPB from TMS_Data.Payments py join (select loan_id,sum(if(pc.payment_type rlike 'LZD',pc.payment_amt-pc.paid_amt,0)) as OutstandingInterest,sum(if(pc.payment_type rlike 'FEE',pc.payment_amt-pc.paid_amt,0)) as OutstandingFee from TMS_Data.PaymentCalendar as pc where loan_id in ('"+str(lid)+"') and override_flag=0 group by loan_id) pc on py.loan_id = pc.loan_id;"
			invbal=curr.execute(inv_bal_q)
                	inv_baldetail=curr.fetchall()
			Inv_Bal = str(inv_baldetail[0][1])
			OPB = str(inv_baldetail[0][2])
			status_q="select status_cd,closure_cd,end_dt from GCMS_Data.gcm_case join TMS_Data.Collection_Info on loan_id=entity_id where dca_name='MH' and case_type_cd='LOAN' and loan_id='"+str(lid)+"'"
	                status_ex=curr.execute(status_q)
        	        Statusdetail=curr.fetchall()
			status = str(Statusdetail[0][0])
			closure = str(Statusdetail[0][1])
			Enddate = str(Statusdetail[0][2])
			values=(str(lid),str(dt),str(MH_transum),str(TMS_Balsum),str(Inv_Bal),str(OPB),str(status),str(closure),str(Enddate))
			print values
	                inst_values = "insert into ukl_collections.MHMonthlyAduit values %s" % str(values)
			print inst_values
                	ins.execute(inst_values)
	        print "mh finished"
	if opt == 2:
		Numbers_q = "select count(*) from ukl_collections.MHMonthlyInvoice MI left join TMS_Data.Transactions on (ClientID = loan_id and TranID = merchant_txn_id) where date(MI.create_dt) = curdate() and loan_id is not NULL;"
		Number_ex = curr.execute(Numbers_q)
		Number_dl = curr.fetchall()
		Numbers = str(Number_dl[0][0])
		print "The number of Transactions matched is "+str(Numbers)
		NTNumbers_q = "select count(*) from ukl_collections.MHMonthlyInvoice MI left join TMS_Data.Transactions on (ClientID = loan_id and TranID = merchant_txn_id) where date(MI.create_dt) = curdate() and loan_id is NULL;"
                NTNumber_ex = curr.execute(NTNumbers_q)
                NTNumber_dl = curr.fetchall()
                NTNumbers = str(NTNumber_dl[0][0])
		print "The number of Transactions not updated is " +str(NTNumbers)		
		if NTNumbers != '0':
			display = raw_input('Do you want to display the transactions which is not updated (yes/no):')
			print display
			print display.upper()
			if display.upper() in ('Y','YES'):
				Display_q = "select ClientID,TranID,Net_Collected,Collected_Date,loan_id,merchant_txn_id,tran_dt from ukl_collections.MHMonthlyInvoice MI left join TMS_Data.Transactions on (ClientID = loan_id and TranID = merchant_txn_id) where date(MI.create_dt) = curdate() and loan_id is NULL order by TranID;"
				Display_ex = curr.execute(Display_q) 
				Display_dl = curr.fetchall()
				Display = list(Display_dl)
				if Display:
					for i in Display:
						print "'%s'  '%s'  '%s'  '%s'  '%s'  '%s'  '%s' " %(i[0],i[1],i[2],i[3],i[4],i[5],i[6])
#						print '\n'
				else:
					print "All the Transaction has been updated successfully"
				update_tran = raw_input('Do you need to post the Missed Transaction (yes/no):')
				if update_tran.upper() in ('Y','YES'):
					user_name = raw_input('Enter your name:')
					if user_name in ('SHEIK.H','SENTHIL'):
						passwd = raw_input('Enter password:')
						if passwd == 'post!23':
							for i in Display:
								tran_type = raw_input('Enter Transaction Type:\n1.Debit\t2.Waived\t3.Refund:')
								values = (str(i[0]),'MH',str(i[1]),str(i[3]),str(i[2]),str(tran_type),'','',str(dtnow),str(user_name))
								ins_query = 'insert into ukl_collections.FileUpdate_Payment values %s' % str(values)		
								print ins_query
								confirm_q = raw_input('Do you need to proceed with this query(yes/no):')
								if confirm_q.upper() in ('Y','YES'):
									ins_execute = curr.execute(ins_query)
								else:
									print 'The above Query is not executed'
					else:
						print 'You are not authorized to post the payments.'

if __name__ =='__main__':
	print "Choose Collection Agency you option are (1,2)"
	print "1.CRS \n 2.MH \n"
	option=raw_input()
	if option=='1':
		print "Please keep CRS billing report in .CSV format"
		crsfile=raw_input("Enter the file name with path Ex : /tmp/acb.csv")
		updatecrs(crsfile)
		print "values are inserting in derived table "
		crsprocess()
		print "Derived table values are inserted"
	elif option=='2':
		print "Please keep MH billing report in .xls format"
		load = raw_input("Do you want to load the file (yes/no):").upper()
		if load in ('Y','YES'):
			mhfile=raw_input("enter the file name with path\nex : /tmp/acb.csv\n\nPath:")
			updatemh(mhfile)
			print"values are inserting in derived table"
		mhprocess()
		print "Derived table values are inserted"
	else:
		print "Sorry dude you have selected wrong option Try again"
	

#100% confirmed none updated transactions for current month
#select loan_id, crs_paid,tms_paid,CRS_CB,(crs_paid+CRS_CB) sum, Inv_bal,cur_status,status_coll from ukl_collections.crs_bill_ssnlogic_new where tran_dt = '2014-04' and CRS_CB!=Inv_bal and crs_paid!=tms_paid having sum = Inv_bal; 

#Suspect previous transactions not posted but current month transactions posted successfully
