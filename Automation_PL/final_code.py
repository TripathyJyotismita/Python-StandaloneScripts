#jobinfld3,jobinfolder
#TOTAL NO OF BUILDS :  4
#TOTAL NO OF JOBS:  7
import json
from jenkinsapi.jenkins import Jenkins
import json
from subprocess import Popen
import shlex
import subprocess
import cfgdata

uname= cfgdata.user
passwd = cfgdata.passwd
token = cfgdata.token
localhost = cfgdata.localhost
dburl = cfgdata.dburl
dbname=cfgdata.dbname
blrjenkins='https://jenkins-blr.rndit.intra.lighting.com/'
blrusername='600038198'
blrpasswd='Lighting@123'
blrserver = Jenkins(localhost,username=uname,password=token)
print 'loggedinto vlrserver'
	
curl -X GET -u 600038198:Lighting@123  https://jenkins-blr.rndit.intra.lighting.com
	
def get_build_info():
	#os.environ['http_proxy'] = "http://zscaler.proxy.intra.lighting.com:9480"
	#os.environ['https_proxy'] = "http://zscaler.proxy.intra.lighting.com:9480"
	server = Jenkins(localhost,username=uname,password=token)
	#blrserver = Jenkins(localhost,username=uname,password=token)
	#print(server.get_job("dtf_buildjob"))
	#job = 'http://localhost:8080/job/dtf_buildjob/api/json/'
	#print job
	
	name_of_jobs=[]
	jobs = server.get_jobs()
	#print jobs
	available_job_list=list(jobs)
	items_found=[]
	import re
	pattern=re.compile(r'\/')
	for ele in available_job_list:
		#print ele
		items_found.append(ele[0])
		for e in ele:
			name_of_jobs.append(e)
	#print items_found
	
	job_in_folder=[]
	#print name_of_jobs
	#st='folder1/folder2/folder3/jobinfld3'
	for items in items_found:
		match=re.findall(pattern,items)
		#print match
		if match:
			job_in_folder.append(items.split('/')[-1])
	print ','.join(job_in_folder)
	totalBuildCount = 0
	job_info=[]

	for job in server.keys():
		builds = server[job].get_build_dict()
		job_info.append(server[job])
		totalBuildCount += len(builds)

	print "TOTAL NO OF BUILDS : ", totalBuildCount
	#print job_info
	print "TOTAL NO OF JOBS: ", len(job_info)

	#1: 'http://localhost:8080/job/dtf_test/1/',

	#job = server.get_job(JOB_NAME)  # or j[JOB_NAME]
	#build = job.get_build(BUILD_ID)
	#print build.get_resultset()

	queue_info = server.get_queue()

get_build_info()
