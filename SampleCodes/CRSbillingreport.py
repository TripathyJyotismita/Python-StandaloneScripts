import os
import sys
import csv
os.environ['PYTHON_EGG_CACHE'] = os.getcwd()
os.sys.path.append("/public/collections/codes")
import commands
import CollectionsftpUP
import traceback
import paramiko
import datetime
import MySQLdb
import smtplib

cur_date=datetime.datetime.today()
conn = MySQLdb.connect('localhost','opsuser','OPS!@#','ukl_collections')
curs = conn.cursor()
#start_dt = (datetime.datetime.today()-datetime.timedelta(6)).strftime('%d%m%y')
#end_dt = (datetime.datetime.today()-datetime.timedelta(1)).strftime('%d%m%y')
filedate = datetime.datetime.today().strftime('%Y%m%d')
tdate = datetime.datetime.now().strftime("%Y%m%d")
#tdate = '20140314'
#filedate = '20140314'

def CollectionSftp(sftp_path,local_path,Agency):
        try:
                host = CollectionsftpUP.DCA[Agency]['host']
                port = int(CollectionsftpUP.DCA[Agency]['port'])
                transport = paramiko.Transport((host, port))
                password = CollectionsftpUP.DCA[Agency]['password']
                username = CollectionsftpUP.DCA[Agency]['user']
                transport.connect(username = username, password = password)
                sftp = paramiko.SFTPClient.from_transport(transport)
                print sftp_path
                try:
                        remotepath = sftp_path+file
                        print remotepath
                        localpath = local_path+file
                        print localpath
                        sftp.get(remotepath,localpath)
                except:
                        print str(traceback.format_exc())
        except:
                print str(traceback.format_exc())

def data_align_table(local_path):
	csvfile = open(local_path+file)
	csv_read = csv.reader(csvfile,delimiter = ",",quotechar = "'")
	csv_read.next()
	for row in csv_read:
        #        row=[ i.replace("'","") for i in row]
                if row[0] == 'Totals':
                        return 1
                else:
                	if len(row) != 9 and len(row) <9:
                                row.append('')
                        row.append(cur_date)
                        print row
                        Query_LL = "select a.Leadid,a.LoanID from uklsoft.LoanStatus a where a.AgreementNumber='%s'" % str(row[3])
			print Query_LL
                        curs.execute(Query_LL)
                        qll = curs.fetchone()
			values = (str(row[0]),str(row[1]),str(row[2]),str(row[3]),str(row[4]),str(row[5]),str(row[6]),str(row[7]),str(row[8]),str(row[9]),str(qll[0]),str(qll[1]))
			print values
			inst_values = 'insert into ukl_collections.CRSBillingReport values %s' % str(values)
#			inst_values = 'insert into ukl_collections.CRSAccount_2013 values %s' % str(values)
			print inst_values
			curs.execute(inst_values)


def report():
	query = "select count(*) from ukl_collections.CRSBillingReport where date(inserttime)=curdate();"
	curs.execute(query)
	reportq = curs.fetchone()
	sub = 'Collection :: CRS Billing Report'
	htmlmsg = "Hi All,<br><br>The report has been loaded into table successfully.<br><br>No. of records loaded from file dated "+str(filedate)+" is "+str(reportq[0])+".<br><br>Regards,<br>Sheik H." 
	sender = ['sheik.h@global-analytics.com']
        receiver = ['senthil.selva@global-analytics.com']
        cc = ['rajkumar.v@global-analytics.com','sheik.h@global-analytics.com']
	receiver = ['sheik.h@global-analytics.com']
	cc = []
        message="""From:<sheik.h@global-analytics.com>
To:"""+','.join(receiver) + """
cc:"""+','.join(cc) + """
MIME-Version:1.0
Content-type: text/html
Subject: """ + sub + """
"""+htmlmsg
        smtpObj=smtplib.SMTP('localhost')
        smtpObj.sendmail(sender,receiver+cc,message)
	print 'sucess'

	

if __name__ == "__main__":
        Agency = 'CRS'
        print filedate
	sftp_path = '/uploads/crs_to_ls/'+tdate+'/'
        local_path = '/public/collections/Process_codes/Billing_Analysis_Report/'+Agency+'/'+tdate+'/'
	commands.getoutput('mkdir '+local_path)
	file = 'billing_analysis '+filedate+'.csv'
#	file = 'billing anaysis '+filedate+'.csv'
        CollectionSftp(sftp_path,local_path,Agency)
	data_align_table(local_path)
	report()
