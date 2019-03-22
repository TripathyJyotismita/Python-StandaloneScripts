# This code has been imported in Activity report for CRS, so changes made here will affect that too. 
# Please make necessary change there also. Thanks - Sheik H.

import os
import sys
import datetime
import time
import traceback
import csv
import commands
import CollectionsftpUP as CC
import paramiko
os.environ['PYTHON_EGG_CACHE'] = os.getcwd()
import MySQLdb
conn = MySQLdb.connect('localhost','opsuser','OPS!@#','ukl_collections')
curs = conn.cursor()
curdate=datetime.datetime.now()
today = datetime.datetime.now().strftime("%Y%m%d")
#today = '20130902'

def CollectionSftp(sftp_path,local_path,file,Agency,filestatus):
        try:
                host = CC.DCA[Agency]['host']
                port = int(CC.DCA[Agency]['port'])
                transport = paramiko.Transport((host, port))
                password = CC.DCA[Agency]['password']
                username = CC.DCA[Agency]['user']
                transport.connect(username = username, password = password)
                sftp = paramiko.SFTPClient.from_transport(transport)
                print sftp_path
       #         for item in sftp.listdir(sftp_path):
       #                 print item
       #                 if curdate in item:
                try:
			remotepath = sftp_path+file
                        print remotepath
                        localpath = local_path+file
                        print localpath
			if filestatus == 1:
				print 'Downloading the file'
				sftp.get(remotepath,localpath)
			elif filestatus == 2:
				print 'Uploading the file'
				sftp.put(localpath,remotepath)
		except:
                	print str(traceback.format_exc())
        except:
                print str(traceback.format_exc())


def write_to_csv_Closure(Agency,local_path):
	try:
		C_detail_query = "select loan_id,ClosureDate,Closure_cd,Agent from ukl_collections.FileUpdate_Closure where Agency = '%s' and date(create_dt)=curdate()"% str(Agency)
		print C_detail_query
		curs.execute(C_detail_query)
		Cdetail = curs.fetchall()
		Cdetail_list = list(Cdetail)
		if Cdetail_list:
			Cfile = local_path+Agency+'_LS_Closure_Missed_'+today+'.csv'
			csv_writer = csv.writer(open(Cfile,"wb"))
			for Cdata in Cdetail_list:
				print Cdata
				csv_writer.writerow(Cdata)
			return 1
		else:
			return 0
	except:
		print str(traceback.format_exc())


def write_to_csv_Payment(Agency,local_path):
	try:
		detail_query = "select loan_id,TransactionID,PaymentDate,PaymentAmount,PaymentType,DCtrails4,CollectedBy from ukl_collections.FileUpdate_Payment where Agency = '%s' and date(create_dt)=curdate()"% str(Agency)	
		print detail_query
		curs.execute(detail_query)
		detail = curs.fetchall()
		print detail
		detail_list = list(detail)
		print detail_list
		if detail_list:
			file = local_path+Agency+'_LS_Payment_Missed_'+today+'.csv'
			csv_writer = csv.writer(open(file,"wb"))
			for data in detail_list:
				print data
				csv_writer.writerow(data)
			return 1
		else:
			return 0
	except:
		print str(traceback.format_exc())

def append_to_csv(Agency,local_path,filedca):
	file_a = local_path+filedca
	f1 = open(file_a,'a')
	csv_writer = csv.writer(f1, delimiter=',')
	filel = local_path+filedca[:-12]+'Missed_'+filedca[-12:]
	csv_read = csv.reader(open(filel))
	for row in csv_read:
		print row
		csv_writer.writerow(row)
	f1.close()
		

if __name__=='__main__':
	try:
		lst_agency = CC.Agency_list
		for Agency in lst_agency:
			local_path = '/home/sheik.h/MissingPayments/'+Agency+'/'+today+'/'
			sftp_path = CC.DCA[Agency]['path_recv']+today+'/'
			commands.getoutput('mkdir '+local_path)
			Pvalue = write_to_csv_Payment(Agency,local_path)
			print Pvalue
			if Pvalue == 1:
				filedca = Agency+'_LS_Payment_'+today+'.csv'
				CollectionSftp(sftp_path,local_path,filedca,Agency,1)
				commands.getoutput('cp '+local_path+filedca+' '+local_path+Agency+'_LS_Payment_'+today+'BCK.csv')
				append_to_csv(Agency,local_path,filedca) 
				CollectionSftp(sftp_path,local_path,filedca,Agency,2)
				print 'Download and write'
			elif Pvalue == 0:
				print 'No Data'
			Cvalue = write_to_csv_Closure(Agency,local_path)
			print Cvalue
			if Cvalue == 1:
				filedca = Agency+'_LS_Closure_'+today+'.csv'
				CollectionSftp(sftp_path,local_path,filedca,Agency,1)
				commands.getoutput('cp '+local_path+filedca+' '+local_path+Agency+'_LS_Closure_'+today+'BCK.csv')
				append_to_csv(Agency,local_path,filedca)
				CollectionSftp(sftp_path,local_path,filedca,Agency,2)
				print 'Download and write'
			elif Cvalue == 0:
				print 'No Data'
	except:
		print str(traceback.format_exc())
