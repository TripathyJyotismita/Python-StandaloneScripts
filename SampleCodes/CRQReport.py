import os
import sys
import csv
os.environ['PYTHON_EGG_CACHE'] = os.getcwd()
os.sys.path.append("/public/collections/codes")
import commands
import CollectionsftpUP
import traceback
import datetime
import MySQLdb
import smtplib

conn = MySQLdb.connect('localhost','opsuser','OPS!@#','ukl_collections')
curs = conn.cursor()

tdate = datetime.datetime.now().strftime("%Y%m%d")
AR=0
AC=0
WCR=0
CNR=0


def sendmail(sender,receiver,sub,htmlmsg,cc=[]):
        message="""From:<sheik.h@global-analytics.com>
To:"""+','.join(receiver) + """
cc:"""+','.join(cc) + """
MIME-Version:1.0
Content-type: text/html
Subject: """ + sub + """
"""+htmlmsg
        smtpObj=smtplib.SMTP('localhost')
        smtpObj.sendmail(sender,receiver+cc,message)


def gen_table(Header,Tuple,opt="center"):
    html_str=''
    if Tuple:
        if Header:
            html_str+='<table style="font-size:12px;" border=1 cellspacing=0><tr>'
            for value in Header:
                html_str+='<th bgcolor="yellowgreen" align="'+opt+'">'+str(value)+'</th>'
            html_str+='</tr>\n'
        for rows in Tuple:
            html_str+='\n<tr>'
            for value in rows:
                html_str+='<td align="'+opt+'">'+str(value)+'</td>'
            html_str+='</tr>'
        html_str+='</table>'
    else:
        html_str='<br /> --No Data--'
    return html_str



if __name__ == "__main__":
	TMSquery = "select loan_id, ClosureReason from REPORTS_Data.ClosureFile where generated_by='Web Service';"
	curs.execute(TMSquery)
	TMScount = curs.fetchall()
	WSdetail = list(TMScount)
	print WSdetail
	statusquery = "select CF.ClosureReason,CI.dca_name,count(*) from REPORTS_Data.ClosureFile CF join TMS_Data.Collection_Info CI using(loan_id) where end_dt is NULL and closure_cd is NULL and generated_by='Web Service' and dca_name!='WF' group by 2,1;"
	curs.execute(statusquery)
	statuscount = curs.fetchall()
	Header = ['Reason','DCA Name','Count']
	query = "select CF.loan_id,CF.ClosureDate,CF.ClosureReason,CI.dca_name,CI.start_dt,CI.end_dt,CI.closure_cd from REPORTS_Data.ClosureFile CF join TMS_Data.Collection_Info CI using(loan_id) where end_dt is NULL and closure_cd is NULL and generated_by='Web Service' and dca_name!='WF' order by 4,3"
	curs.execute(query)
	Count = curs.fetchall()
	Header1 = ['Loan ID','TMS Date','Reason','DCA Name','Start DT','End DT','Closure']
	print Count
	for row in WSdetail:
		print row[1]
		print row[0]
		getquery = "select dca_name,end_dt,closure_cd,loan_id from TMS_Data.Collection_Info where loan_id = '%s'  order by end_dt desc limit 1;" % str(row[0])
		print getquery
		curs.execute(getquery)
		getdetail = curs.fetchone()
		print getdetail
		if getdetail is not None:
			getlist = list(getdetail)
			print getlist
			if getlist[1]:
				if getlist[2] in ('CRQ','CLI_REQ','RTC'):
					print 'Account Returned'
					AR = AR + 1
				elif getlist[2] == row[1]:
					print 'Account Closed'
					AC = AC +1
				else:	
					print 'Wrong Closure Received'
					WCR = WCR + 1
			else:
				print 'Closure not Received'
				CNR = CNR + 1
	values = (AR, AC, WCR, CNR)
	print values
	print AR, AC, WCR, CNR, len(Count)
	print AR+AC+WCR+CNR
	print len(WSdetail)
	mailcontent = "<font face='Cambria' size = 2>Hi All,<br><br>Please find the Account Recalled Closure status between us and DCA.<br><br />Total Re-Called Accounts: "+str(len(WSdetail))+"<br><br /> Account ReCalled as (CRQ,CLI_REQ,RTC): "+str(AR)+"<br><br/>Account Closed as other end status: "+str(AC)+"<br><br/>Account Closed with different Closure code: "+str(WCR)+"<br><br />Still un-closed accounts: "+str(len(Count))+"<br /><br><b>Status-wise Unclosed Accounts:<b /><br/>"+gen_table(Header,statuscount)+"<br/><br/>Regards,<br/>Sheik H."
	to=['senthil.selva@global-analytics.com']
	cc=['rajkumar.v@global-analytics.com','sheik.h@global-analytics.com']
	to = ['sheik.h@global-analytics.com']
	cc = []
	sender = 'sheik.h@global-analytics.com'
	subject='Collection :: Re-called Account Closure status'
	sendmail(sender,to,subject,mailcontent,cc)
