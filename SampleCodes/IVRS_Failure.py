import DBConfig
from Lead_Activity_Report_ss import *

conn = MySQLdb.connect(DBConfig.SLAVE_DB,DBConfig.OPS_USERNAME,DBConfig.OPS_PWD,'uklsoft')
curs = conn.cursor()

def sendmail(sub,cont):
	sender='uklops@global-analytics.com'
	receiver=['ivrs@globalanalytics.com','opsteam@globalanalytics.com','uklops@global-analytics.com']
#	receiver =['pandithdurai.g@global-analytics.com']
	strto=','.join(receiver)
	print strto
#	receiver =['krishnakumar@global-analytics.com']
	message="""From:<uklops@global-analytics.com>
To:"""+strto+"""
MIME-Version:1.0
Content-type: text/html
Subject: """+sub+"""
"""+cont

	smtpObj=smtplib.SMTP('localhost')
	smtpObj.sendmail(sender,receiver,message)


if __name__=='__main__':
#	IVRS_Failure="select AgreementNumber, case when AutoApproved then 'AutoApproved' when FastApproved and AutoApprovalEligible >0 then 'FADC' when FastApproved and FastApprovalEligible > 0  then 'FAEC' when FastApproved =0 and AutoApproved =0 then 'Normal' end Type  ,ServiceCode, Decision ,ErrorMsg,date(DateTime)   from IVRS_Adeptra a join ClientResponse b using (LeadID)  where Status ='failure' and DateTime > curdate() and date(DateTime)!='2012-01-31' and ErrorMsg!='Invalid lead_id' order by 2,5;"
#	IVRS_Failure="select leadid, status, Error from IcenetResponse where status='failure' limit 10";
	IVRS_Failure="select AgreementNumber, if(c.servicecode like '7%', 'Zebit','Lending Stream') Brand, case when AutoApproved then 'AutoApproved' when FastApproved and AutoApprovalEligible >0 then 'FADC' when FastApproved and FastApprovalEligible > 0  then 'FAEC' when FastApproved =0 and AutoApproved =0 then 'Normal' end Type  ,a.ServiceCode, Decision ,ErrorMsg,date(DateTime)   from IVRS_Adeptra a join ClientResponse b using (LeadID) join ClientRequest c using (LeadID)  where Status ='failure' and DateTime > curdate() and date(DateTime)!='2012-01-31' and ErrorMsg!='Invalid lead_id' order by 2,5"
	curs.execute(IVRS_Failure)
	IVRS_FailureData=curs.fetchall()
	print IVRS_FailureData
	if IVRS_FailureData:
	        Header=['Agreement Number','Brand','Type','ServiceCode','Status','ErrorMsg','Date']
        	cont="Hi Team, <br /><br /> PFB the failure while loading to IVRS  <br /><br /> "+gen_table(Header,IVRS_FailureData)+" <br /><br />Regards,<br />OPS."
	        sub="Alert : Failure while loading to IVRS"
	        sendmail(sub,cont)
	
