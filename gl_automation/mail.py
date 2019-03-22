import smtplib,os,subprocess
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import Encoders
from automationUtil import cfgdata
def send(body,sub,c,fn):
	passwd=cfgdata['password']['mail']
	p=os.popen("echo %s | openssl enc -aes-128-cbc -a -d -salt -pass pass:121"%(passwd)).read()
	password=p.strip()
	fromaddr=cfgdata['mailingList']['from']
	toaddr=cfgdata['mailingList']['to']
	ccaddr=cfgdata['mailingList']['cc']
	msg = MIMEMultipart()
	msg['From'] = fromaddr
	msg['To'] = ', '.join(toaddr)
	msg['Cc']=', '.join(ccaddr)
	msg['Subject'] = "OSS Automation Progress | "+sub
	if c==1:
		part = MIMEBase('application', "octet-stream")
		part.set_payload(open(fn, "rb").read())
		Encoders.encode_base64(part)
		part.add_header('Content-Disposition', 'attachment; filename="outputfile.txt"')

		msg.attach(part)
	msg.attach(MIMEText(body, 'plain'))
	try:   
		server = smtplib.SMTP('smtp.office365.com', 587)
		server.starttls()
		server.login(fromaddr,password)
		text = msg.as_string()
		server.sendmail(fromaddr, toaddr+ccaddr, text)
		server.quit()
	except:
		print('ERROR: Unable to connect to mail server')

