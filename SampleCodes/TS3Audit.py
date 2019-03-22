import csv
import re
import sys
from time import strptime
from HTMLUtil import *
import os
os.environ['PYTHON_EGG_CACHE'] = "/public/gdp/OPSScript/PayoutAudit/"
import MySQLdb
import datetime
import Mail
import traceback
class PayoutAudit:
        def __init__(self):
                insert_db=MySQLdb.connect('db1.localdomain','ukluser','GL0b@lL3nd!ng','ukloffline')
                self.insert_curs=insert_db.cursor()
		self.insert_curs.execute('set autocommit=1')
	        select_db=MySQLdb.connect('db1.localdomain','uklops','uklDB1@3','ukloffline')
		self.selectcurs=select_db.cursor()
                self.csv_list=[]
                self.FileReader()

        def FileReader(self):
                files=os.popen("ls /public/gdp/OPSScript/PayoutAudit/TS3Transactions/|grep -v Archive").read().split("\n")[:-1]
                self.curdate=datetime.datetime.now().strftime('%Y-%m-%d')
                for file in files:
                        print '/public/gdp/OPSScript/PayoutAudit/TS3Transactions/'+str(file)
                        print file
                        csv_reader=csv.reader(open('/public/gdp/OPSScript/PayoutAudit/TS3Transactions/'+str(file)))
                        for row in csv_reader:
                                try:
                                        row.append(self.curdate)
                                        self.csv_list.append(row)
                                except:
                                        print ""
					print traceback.format_exc()
                        self.FileProcess()
			
#			print "mv "+str(file)+" /public/gdp/OPSScript/PayoutAudit/TS3Transactions_bck/"
			os.system("mv /public/gdp/OPSScript/PayoutAudit/TS3Transactions/"+str(file)+" /public/gdp/OPSScript/PayoutAudit/TS3Transactions_bck/")
 #               print self.csv_list

        def FileProcess(self):
            
               if self.csv_list[0][1].strip()=='Individual Transactions':
                        flag=0
                        for rows in self.csv_list[0:-1]:
                                print rows
                                if flag and rows[0]!="":

                                        rows[0]=rows[0].split(" ")[3]+"-"+str(strptime(rows[0].split(" ")[2],'%b').tm_mon).zfill(2)+'-'+rows[0].split(" ")[1]
                                        rows[1]=re.sub('[^A-Za-z0-9.]+', '',rows[1])
                                        insertquery="insert into ukloffline.TS3TransactionFile values %s"%str(tuple(rows))
                                        print insertquery
                                        try:
                                               print "Trying to Insert"
                                               self.insert_curs.execute(insertquery)
                                        except:
                                               print traceback.format_exc()
                                               pass

                                if 'Submission' in str(rows):
                                        flag =1
               else:
                        print "Wrong Transactions File"
               self.csv_list=[]
 

        def From_To_Date(self):
#		if sys.argv[1] and sys.argv[2]:
#			self.from_date=sys.argv[1]
#			self.to_date=sys.argv[2]
	
	        query="select date(max(PayoutDate)),date(min(PayoutDate)) from ukloffline.TS3TransactionFile where date(inserttime)='%s'"%(self.curdate)
	        print query
	        self.selectcurs.execute(query)
	        self.to_date,self.from_date=self.selectcurs.fetchone()
#		self.from_date='2014-05-18'
#		self.to_date='2014-05-19'
                print self.to_date,self.from_date

	def PRvsTS3(self):
		query="select tmp.status,TS3Count,PRCount,FPSCount from (select status ,count(*)TS3Count from TS3TransactionFile where date(PayoutDate) between '%s' and '%s' group by 1)tmp join (select status ,count(*)PRCount from ukloffline.payout_paymentreport where date(currentdatetime) between '%s' and '%s' group by 1)tmp1 on if(tmp.status='Failure','Failed',tmp.status)=tmp1.status join (select case when CurrentStatus in ('Active','Complete','Finished Early','Withdrawal','Withdrawn') then 'Success' when CurrentStatus not in('Active','Complete','Finished Early','Withdrawal','Withdrawn') then 'Failure' end Status,count(*)FPSCount from uklsoft.FasterPayments join uklsoft.LoanStatus using(AgreementNumber)where date(currenttime) between  '%s' and '%s' and PayoutClient='TS3' group by 1)tmp3 on tmp.status=tmp3.status order by 1 desc"%(self.from_date,self.to_date,self.from_date,self.to_date,self.from_date,self.to_date)
		self.selectcurs.execute(query)
		print query
		self.TS3Numbers=self.selectcurs.fetchall()
		print "Result"
		print self.TS3Numbers

        def TS3vsFPS(self):
#                query="select TS3T.AgreementNumber from uklsoft.FasterPayments FA right join TS3TransactionFile TS3T on FA.AgreementNumber=TS3T.AgreementNumber where date(PayoutDate) between '%s' and '%s' and FA.AgreementNumber is NULL"%(self.from_date,self.to_date)
		query="select NFPS.*,CurrentStatus from (select b.agreementnumber,PayoutClient,if(status is NULL,'Failed',status)Status from (select AgreementNumber,status from ukloffline.TS3TransactionFile where date(PayoutDate)between '%s' and '%s' and status='SUCCESS')a right join (select AgreementNumber,PayoutClient from uklsoft.FasterPayments where date(CurrentTime)between '%s' and '%s' and payoutclient='TS3')b on a.agreementnumber=b.agreementnumber where a.agreementnumber is NULL)NFPS left join uklsoft.LoanStatus using(AgreementNumber)"%(self.from_date,self.to_date,self.from_date,self.to_date)
		query="select tmp.AgreementNumber,Amount,PD from (select AgreementNumber,Amount,date(payoutdate)PD from ukloffline.TS3TransactionFile where date(payoutdate) between '%s' and '%s')tmp left join (select AgreementNumber,`Payout Amount` PA,date(Currenttime) from uklsoft.FasterPayments where date(currenttime) between '%s' and '%s' and PayoutClient='TS3')tmp1 on tmp.AgreementNumber=tmp1.AgreementNumber and tmp1.PA=tmp.Amount where tmp1.AgreementNumber is NULL"%(self.from_date,self.to_date,self.from_date,self.to_date)
                print query
                self.selectcurs.execute(query)
                self.FPSMissingList=self.selectcurs.fetchall()

        def FPSvsTS3(self):
#                query="select FA.AgreementNumber from uklsoft.FasterPayments FA left join TS3TransactionFile TS3T on FA.AgreementNumber=TS3T.AgreementNumber where date(currenttime) between '%s' and '%s' and PayoutClient='TS3' and TS3T.AgreementNumber is NULL"%(self.from_date,self.to_date)
		query="select tmp1.AgreementNumber,PA,PD1 from (select AgreementNumber,Amount,date(payoutdate)PD from ukloffline.TS3TransactionFile where date(payoutdate) between '%s' and '%s')tmp right join (select AgreementNumber,`Payout Amount` PA,date(Currenttime)PD1 from uklsoft.FasterPayments where date(currenttime) between '%s' and '%s' and PayoutClient='TS3')tmp1 on tmp.AgreementNumber=tmp1.AgreementNumber and tmp1.PA=tmp.Amount where tmp.AgreementNumber is NULL"%(self.from_date,self.to_date,self.from_date,self.to_date)
                print query
                self.selectcurs.execute(query)
                self.TS3MissingList=self.selectcurs.fetchall()

	def TS3vsPR(self):
		query="select tmp.AgreementNumber,payoutdate,Amount from (select AgreementNumber,payoutdate,Amount from ukloffline.TS3TransactionFile where date(payoutdate) between '%s' and '%s')tmp left join (select AgreementNumber from ukloffline.payout_paymentreport where date(currentdatetime) between '%s' and '%s')tmp1 using(AgreementNumber) where tmp1.AgreementNumber is NULL"%(self.from_date,self.to_date,self.from_date,self.to_date)
		print query
		self.selectcurs.execute(query)
		self.PRMissingList=self.selectcurs.fetchall()

	def PRvsTS3_Mismatches(self):
		query="select tmp1.AgreementNumber,currentdatetime,tmp1.Amount from (select AgreementNumber,payoutdate,Amount from ukloffline.TS3TransactionFile where date(payoutdate) between '%s' and '%s')tmp right join (select AgreementNumber,Amount,currentdatetime from ukloffline.payout_paymentreport where date(currentdatetime) between '%s' and '%s')tmp1 using(AgreementNumber) where tmp.AgreementNumber is NULL"%(self.from_date,self.to_date,self.from_date,self.to_date)
		print query
		self.selectcurs.execute(query)
		self.TS3vsPRMissingList=self.selectcurs.fetchall()

        def MismatchLoanamount(self):
#               query="select AgreementNumber,Amount,`Payout Amount` from TS3TransactionFile join uklsoft.FasterPayments using(AgreementNumber) where date(PayoutDate) between '%s' and '%s' and Amount!=`Payout Amount` and PayoutClient='TS3' "%(self.from_date,self.to_date)
		query="select AgreementNumber,PA,Amount from (select AgreementNumber,`Payout Amount` PA,currenttime from uklsoft.FasterPayments where date(currenttime) between '%s' and '%s' and PayoutClient='TS3')tmp join (select AgreementNumber,Amount from ukloffline.TS3TransactionFile where date(PayoutDate) between '%s' and '%s')tmp2 using(AgreementNumber) where PA!=Amount"%(self.from_date,self.to_date,self.from_date,self.to_date)
                print query
                self.selectcurs.execute(query)
                self.LAMisamtchAgreements=self.selectcurs.fetchall()
		
        def resultString(self):
                check_list = []
		self.htmlstr =""
		if self.TS3Numbers:
			print "TS3Numbers"
			self.htmlstr ="<br><br>Payment Report and TS3 Table comparison<br><br>"
			self.htmlstr += str(Table(self.TS3Numbers,['Status','TS3Count','PRCount','FPSCount']))
			
                if self.FPSMissingList:
                        self.htmlstr +="<br><br>Agreement shows as paid in TS3 OTIS but not entered in FasterPayments Table<br><br>"
                        self.htmlstr += str(Table(self.FPSMissingList,['Agreement','Amount','PayoutDate']))
                        check_list.append(["FasterPayment Table Check","NOTOK"])
                else:
                        check_list.append(["FasterPayment Table Check","OK"])
                if self.TS3MissingList:
                        self.htmlstr +="<br><br>Agreement shows as paid in FasterPayments but not showing in TS3 OTIS<br><br>"
                        self.htmlstr += str(Table(self.TS3MissingList,['Agreement','Amount','PayoutDate']))
                        check_list.append(["TS3 Paid Status Check","NOTOK"])
                else:
                        check_list.append(["TS3 Paid Status Check","OK"])
                if self.LAMisamtchAgreements:
                        self.htmlstr +="<br><br>Loan Amount does not match in FasterPayments and in TS3 OTIS<br><br>"
                        self.htmlstr += str(Table(self.LAMisamtchAgreements,['Agreement','PaidAmount','LoanAmount']))
                        check_list.append(["LoanAmountCheck","NOTOK"])
                else:
                        check_list.append(["LoanAmountMatch","OK"])
		if self.PRMissingList:
			self.htmlstr +="<br><br>Agreement present in OTIS but in Payment report<br><br>"
			self.htmlstr += str(Table(self.PRMissingList,['Agreement','PaidDate','LoanAmount']))
			check_list.append(["PaymentReportvsTS3","NOTOK"])
		else:
			check_list.append(["PaymentReportvsTS3","OK"])
		if self.TS3vsPRMissingList:
			self.htmlstr +="<br><br>Agreement present in Payment Report but not in OTIS<br><br>"
			self.htmlstr += str(Table(self.TS3vsPRMissingList,['Agreement','PaidDate','LoanAmount']))
			check_list.append(["TS3vsPaymentreport","NOTOK"])
		else:
			check_list.append(["TS3vsPaymentreport","OK"])
                self.htmlstr = str(Table(check_list,["Check List","Status"]))+ self.htmlstr
                print self.htmlstr

	def sendmail(self):
		Mail.senm("Please find below the Audit between"+str(self.from_date)+" and "+str(self.to_date)+"<br><br>"+str(self.htmlstr),"TS3 Audit ("+str(self.from_date)+"-"+str(self.to_date)+")","/public/gdp/OPSScript/PayoutAudit/mailList.txt")			



if __name__=='__main__':
        PA=PayoutAudit()
        PA.From_To_Date()
	PA.PRvsTS3()
        PA.TS3vsFPS()
        PA.FPSvsTS3()
        PA.MismatchLoanamount()
	PA.TS3vsPR()
	PA.PRvsTS3_Mismatches()
        PA.resultString()
	PA.sendmail()

