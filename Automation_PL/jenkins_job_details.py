#runs only on no proxy
from jenkinsapi.jenkins import Jenkins
import json
from subprocess import Popen
import shlex
import subprocess
import cfgdata

global uname
global passwd
global localhost
global dburl
global dbname
#uname='jyoti_signify1'
#passwd='Jyoti@Jenkin1'
uname= cfgdata.user
passwd = cfgdata.passwd
localhost = cfgdata.localhost
dburl = cfgdata.dburl
dbname=cfgdata.dbname


def get_build_info():
	try:
		#uname= cfgdata.JENKINS_CONFIG.user
		
		totalBuildCount = 0
		job_info=[]
		server = Jenkins(localhost,username=uname,password=passwd)
		plugins = server.get_plugins()
		noofpluginsinstalled= len(plugins)
		print "TOTAL PLUGINS INSTALLED: ", noofpluginsinstalled

		#list of jobs available
		#jobs=server.get_jobs()
		#get the count of each job occurance
		#print list(jobs)
		
		jobs_available=[]
		for job in server.keys():
			#print job
			jobs_available.append(job)
		total_jobs_available=len(jobs_available)
		print "TOTAL NO OF JOBS: ", total_jobs_available
		"""
		this gives no of build for all the available
		for job in server.keys():
			builds = server[job].get_build_dict()
			job_info.append(server[job])
			totalBuildCount += len(builds)
		
		#print job_info
		print "TOTAL NO OF BUILDS : ", totalBuildCount
		"""	
	except jenkins.JenkinsException:
		print 'connections issue'
	except jenkins.BadHTTPException:
		print 'broken HTTP response'
	except jenkins.TimeoutException:
		print "socket timeout"
	finally:
		print 'Job Done !!'
		
get_build_info()