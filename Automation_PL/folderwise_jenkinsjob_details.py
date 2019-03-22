#runs only on no proxy
from jenkinsapi.jenkins import Jenkins
import json,re
from subprocess import Popen
import shlex
import subprocess
import cfgdata
global uname
global passwd
global localhost
global dburl
global dbname

uname= cfgdata.user
passwd = cfgdata.passwd
localhost = cfgdata.localhost
dburl = cfgdata.dburl
dbname=cfgdata.dbname

def json_parsing():
	regex=re.compile(r'folder|Folder')
	jobs_inside_folder=[]
	cmd= 'curl -u '+ uname+':'+passwd +' -X GET '+localhost+'/api/json/ -o json_outfile/api.json'
	p1 = Popen(shlex.split(cmd))

	#with open('C:\\Python27\\json_outfile\\test-json-file.json') as json_file:  
	#	data = json.load(json_file)
	with open('C:\\Python27\\json_outfile\\api.json') as json_file:  
		data = json.load(json_file)
	
		#print data
		for items in data["jobs"]:
			#print items
			for lie in items:
				#print lie
				#li=items["_class"]
				#print '!!!!!1i',items["_class"]
				match=re.findall(regex,(items["_class"]))
				if len(match) >0:
					print len(match)
	
					
					#print items["_class"]
					#print "lie!!!!!!",lie
					folder_name=items["name"]
					#http://localhost:8080/job/testfolder/api/json this is the expected url to hit with folder name
					#http://localhost:8080/job/testfolder/job/nestedfolder-test/api/json
					#http://localhost:8080/job/testfolder/job/dtf_buildjob/api/json
					cmd1 = 'curl -u '+ uname+':'+passwd +' -X GET '+localhost+'/job/'+folder_name+'/api/json/ -o json_outfile/jenkinsfolder.json'
					p2 = Popen(shlex.split(cmd1))
					try:
						with open('C:\\Python27\\json_outfile\\jenkinsfolder.json') as json_file: 
							data = json.load(json_file)
							for i in data["jobs"]:
								job_names=i["name"] #output is=['dtf_buildjob','nestedfolder-test']
								if job_names not in jobs_inside_folder:
									jobs_inside_folder.append(job_names)
								
								#print "JOB found in folders: ",i["name"]
								#print '!!!!!!!!!!!!!!!!!!!!!!++=======',i
					except IOError:
						print 'JSON file not found!'
	print "JOB found in folders:*************** ",((jobs_inside_folder))
						
		
json_parsing()