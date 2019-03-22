import os
import sys
import csv
import datetime, time
from datetime import date,timedelta
import smtplib
import logging
#import traceback
import paramiko
from decimal import Decimal
import soaplib
#Easy to use python library for writing and calling soap web services. Webservices written with soaplib are simple,
#lightweight, work well with other SOAP implementations, and can be deployed as WSGI applications.
# or import SOAPpy

#Report
#Logs
#Abstraction layer <process MTM >BRT>Oracle Job>Validate>
#Utils (jave, oracledb, exe, powerssh)
#


#dbConection
import cx_Oracle
conn = cx_Oracle.connect('username/password@server:port/services')
curs = conn.cursor()
curdate=datetime.datetime.now()
today = datetime.datetime.now().strftime("%Y%m%d")

#billingReport
def report():
	query = "select count(*) from <TABLE_NAME > where date(inserttime)=curdate();"
	curs.execute(query)
	reportq = curs.fetchone()
	sub = 'Affinion :: Daily Billing Report'
	htmlmsg = "Hi All,<br><br>The report has been loaded into table successfully.<br><br>No. of records loaded from file dated "+str(filedate)+" is "+str(reportq[0])+".<br><br>Regards,<br> Billing Team."
    sender = ['jyotismita.tripathy@globallogic.com']
    receiver = ['jyotismita.tripathy@globallogic.com']
    cc = ['<receiver_name>@globallogic.com','receiver_name@globallogic.com']
	receiver = ['<receiver_name>@globallogic.com']
	cc = []
    message="""From:<sender_name@globallogic.com>
    To:"""+','.join(receiver) + """
    cc:"""+','.join(cc) + """
    MIME-Version:1.0
    Content-type: text/html
    Subject: """ + sub + """
    """+htmlmsg
        smtpObj=smtplib.SMTP('localhost')
        smtpObj.sendmail(sender,receiver+cc,message)
	print 'sucess'