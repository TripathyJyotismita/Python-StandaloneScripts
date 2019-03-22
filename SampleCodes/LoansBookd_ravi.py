import os
os.environ['PYTHON_EGG_CACHE'] = "/ukpdl/scripts/OPSScript/"
import datetime
import smtplib
os.sys.path.append("/data/public/gdp/OPSScript/")
os.sys.path.append("/data/public/gdp/trunk/src/ukl/config/")
from Lead_Activity_Summary import *
import pdb
from DBConfig import *
#conn = MySQLdb.connect('db2.localdomain','ukluser','GL0b@lL3nd!ng','uklsoft')
conn = MySQLdb.connect('db2.localdomain','uklops','uklDB1@3','uklsoft')
curs = conn.cursor()
dayval=((datetime.date.today()-datetime.timedelta(1)))
date=dayval.strftime("%d%m%Y")
date1=dayval.strftime("%d-%m-%Y")
yr = dayval.strftime("%Y")
month = dayval.strftime("%m%Y")
def gen_manualtable(Header,Tuple,opt="center"):
    html_str=''
    color1 = "#F0F0F0"
    color2 = "blue"
    if Tuple:
        if Header:
            html_str+='<table border=2 cellspacing=0><tr bgcolor="purple">\n'
            for value in Header:
                html_str+='<th align="'+opt+'"><font face="Arial" color="white">'+str(value)+'</font></th>\n'
            html_str+='</tr>\n'

        for rows in Tuple:
            if "nothing" in str(rows[0]):
                color1 = "yellow"
            html_str+='\n<tr bgcolor="'+color1+'">'
            for value in rows :
                html_str+='<td align="'+opt+'"><font face="Arial" color="'+color2+'">'+str(value)+'</font></td>\n'
            html_str+='</tr>\n'
        html_str+='</table>'
    else:
        html_str='<br /> --No Data--'
    return html_str
#No. Of Loans query
query1="select 'No. of Loans', count(distinct case when (TPFlag =0 or TPFlag is NULL) and (Rf=0 or RF is NULL) and wdf not in (11,12,13,31,32,33)  then a.Leadid end ) New,count(distinct Case when TPFlag>0 then a.Leadid end ) tp, count(distinct Case when RF>0 then a.Leadid end ) RF, count(distinct Case when WDF in (11,12,13,31,32,33) and (TPFlag =0 or TPFlag is NULL) and (Rf=0 or RF is NULL) then a.Leadid end ) WDF, count(distinct AgreementNumber) Total from FasterPayments join IcenetResponse on applicationid = agreementnumber join ClientResponse a using(leadid) join ClientRequest b using (leadid) where BatchFileName rlike '"+date+"' and BatchFileName not like 'REFUND%' and (OPSComment not rlike 'efund' or OPSComment is NULL ) and OPSComment not like '%Cashback%' group by 1;"
#Average Amount query
query2="select 'Average Amount (GBP)',FORMAT(avg(case when (TPFlag =0 or TPFlag is NULL) and (Rf=0 or RF is NULL) and wdf not in (11,12,13,31,32,33)  then `Payout Amount` end),0) New,FORMAT(avg(case when TPFlag>0 then `Payout Amount` end),0) tp, FORMAT(avg(Case when RF>0 then `Payout Amount` end),0) RF, FORMAT(avg(Case when WDF in (11,12,13,31,32,33) and (TPFlag =0 or TPFlag is NULL) and (Rf=0 or RF is NULL) then `Payout Amount` end),0) WDF,FORMAT(avg(`Payout Amount`),0) Total from FasterPayments join IcenetResponse on applicationid = agreementnumber join ClientResponse a using(leadid) join ClientRequest b using (leadid) where BatchFileName rlike '"+date+"' and BatchFileName not like 'REFUND%' and (OPSComment not rlike 'efund' or OPSComment is NULL ) and OPSComment not like '%Cashback%' group by 1;"
#Total Amount query
query3="select 'Total Amount (GBP)',FORMAT(sum(case when (TPFlag =0 or TPFlag is NULL) and (Rf=0 or RF is NULL) and wdf not in (11,12,13,31,32,33)  then `Payout Amount` end),0) New,FORMAT(sum(case when TPFlag>0 then `Payout Amount` end),0) tp, FORMAT(sum(Case when RF>0 then `Payout Amount` end),0) RF, FORMAT(sum(Case when WDF in (11,12,13,31,32,33) and (TPFlag =0 or TPFlag is NULL) and (Rf=0 or RF is NULL) then `Payout Amount` end),0) WDF,FORMAT(sum(`Payout Amount`),0) Total from FasterPayments join IcenetResponse on applicationid = agreementnumber join ClientResponse a using(leadid) join ClientRequest b using (leadid) where BatchFileName rlike '"+date+"' and BatchFileName not like 'REFUND%' and (OPSComment not rlike 'efund' or OPSComment is NULL ) and OPSComment not like '%Cashback%' group by 1;"
#query3="select 'Total Amount (GBP)',sum(case when (TPFlag =0 or TPFlag is NULL) and (Rf=0 or RF is NULL) and wdf not in (11,12,13,31,32,33)  then `Payout Amount` end) New,sum(case when TPFlag>0 then `Payout Amount` end) tp, sum(Case when RF>0 then `Payout Amount` end) RF, sum(Case when WDF in (11,12,13,31,32,33) and (TPFlag =0 or TPFlag is NULL) and (Rf=0 or RF is NULL) then `Payout Amount` end) WDF,sum(`Payout Amount`) Total from FasterPayments join IcenetResponse on applicationid = agreementnumber join ClientResponse a using(leadid) join ClientRequest b using (leadid) where BatchFileName rlike '"+date+"' and BatchFileName not like 'REFUND%' and (OPSComment not rlike 'efund' or OPSComment is NULL ) and OPSComment not like '%Cashback%' group by 1;"
#Treatment-wise loans query
query4="select count(case when AutoApproved then 1 end) AA,count( case when AutoApprovalEligible>0 and FastApproved then 1 end ) FADC,count( case when FastApprovalEligible>0 and FastApproved then 1 end) FAEC,count( case when AutoApproved=0 and FastApproved=0 then 1 end) NA,count( case when AutoApproved then 1 end)+count( case when FastApprovalEligible>0 and FastApproved then 1 end)+count( case when AutoApprovalEligible>0 and FastApproved then 1 end )+count( case when AutoApproved=0 and FastApproved=0 then 1 end) as TL from FasterPayments join IcenetResponse on applicationid = agreementnumber join ClientResponse using (leadid) where BatchFileName rlike '"+date+"' and BatchFileName not like 'REFUND%' and (OPSComment not rlike 'Refund' or OPSComment is NULL ) and OPSComment not like '%Cashback%' and  src in('TMS','TMS_MDL','TMS_LS');"
#Amount as % of Total query
#query5="select 'Amount as % of Total',round(New,0),round(Tp,0),round(RF,0),round(WDF,0),round(Total,0) from (select 'Amount as % of Total',new/Total*100 New,tp/Total*100 Tp,rf/Total*100 RF,wdf/Total*100 WDF,Total/Total*100 Total from (select 'Amount as % of Total',sum(case when (TPFlag =0 or TPFlag is NULL) and (Rf=0 or RF is NULL) and wdf not in (11,12,13,31,32,33)  then `Payout Amount` end)new,sum(case when TPFlag>0 then `Payout Amount` end)tp,sum(Case when RF>0 then `Payout Amount` end)rf,sum(case when (TPFlag=0 or TPFlag is NULL) and (Rf=0 or RF is NULL) and WDF in(11,12,13,31,32,33) then `Payout Amount` end)wdf,sum(`Payout Amount`)Total from FasterPayments join IcenetResponse on applicationid = agreementnumber join ClientResponse a using(leadid) join ClientRequest b using (leadid) where BatchFileName rlike '"+date"' and BatchFileName not like 'REFUND%' and (OPSComment not rlike 'efund' or OPSComment is NULL ) and OPSComment not like '%Cashback%' group by 1)tt)ss;"
query5="select 'Amount as % of Total',Format(round(new/Total*100),0)as New,Format(round(tp/Total*100),0)Tp,Format(round(rf/Total*100),0)RF,Format(round(wdf/Total*100),0)WDF,Format(Total/Total*100,0) Total from (select 'Amount as % of Total',sum(case when (TPFlag =0 or TPFlag is NULL) and (Rf=0 or RF is NULL) and wdf not in (11,12,13,31,32,33)  then `Payout Amount` end)new,sum(case when TPFlag>0 then `Payout Amount` end)tp,sum(Case when RF>0 then `Payout Amount` end)rf,sum(case when (TPFlag=0 or TPFlag is NULL) and (Rf=0 or RF is NULL) and WDF in(11,12,13,31,32,33) then `Payout Amount` end)wdf,sum(`Payout Amount`)Total from FasterPayments join IcenetResponse on applicationid = agreementnumber join ClientResponse a using(leadid) join ClientRequest b using (leadid) where BatchFileName rlike '"+date+"' and BatchFileName not like 'REFUND%' and (OPSComment not rlike 'efund' or OPSComment is NULL ) and OPSComment not like '%Cashback%' group by 1)tt;"
#Payout Amount metrics
query6 = "select 'Total Funding (GBP)', FORMAT(sum(case when BatchFileName rlike '"+date+"' then `Payout Amount` end),0)Today, FORMAT(sum(case when BatchFileName rlike '"+month+"' then `Payout Amount` end),0)MTD, FORMAT(sum(case when BatchFileName rlike '"+yr+"' then `Payout Amount` end),0)YTD from FasterPayments where BatchFileName rlike '"+yr+"' and BatchFileName not like 'REFUND%' and (OPSComment not rlike 'efund' or OPSComment is NULL ) and OPSComment not like '%Cashback%' group by 1;"

Header1=['','New','Topup','React','Wdf','Total']
Header2=['AutoApproval','FADC','FAEC','NA','Total']
Header3 = ['', 'Today', 'MTD', 'YTD']
curs.execute(query1)
Tuple1=curs.fetchall()
curs.execute(query2)
Tuple2=curs.fetchall()
curs.execute(query3)
Tuple3=curs.fetchall()
curs.execute(query5)
Tuple5=curs.fetchall()
Tuple=(Tuple1[0],Tuple2[0],Tuple3[0],Tuple5[0])
print Tuple
curs.execute(query4)
Tuple4=curs.fetchall()
print Tuple4
curs.execute(query6)
Tuple6=curs.fetchall()
print Tuple6

date_format = (datetime.date.today().isoformat())
html_str="Hi All,<br /><br />PFB the Overall segment wise loans: <br/>"+gen_manualtable(Header1,Tuple)+ "<br /><br />PFB the treatment wise for TMS:<br/>"+gen_manualtable(Header2,Tuple4)+"<br /><br />PFB the payout amount metrics: "+gen_manualtable(Header3,Tuple6)+"<br><br/>Regards,<br />UKLOPS."
print html_str
sender='uklops@global-analytics.com'
recv=raw_input('Enter the first name of mail id (ex abc for abc@global-analytics.com)::')
#receiver=['ravikumar.petchiappan@global-analytics.com']
receiver=[recv+'@global-analytics.com']
message="""From:<uklops@global-analytics.com>
To:"""+recv+"""@global-analytics.com
MIME-Version:1.0
Content-type: text/html
Subject:"""+str(Tuple1[0][5])+""" Loans Booked for - """+date1+"""
"""+html_str
message1 = message.replace('< /','<')
smtpObj=smtplib.SMTP('localhost')
smtpObj.sendmail(sender,receiver,message1)
