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

def json_parsing():
	success_count = 0
	nobuilt_count = 0
	cmd= 'curl -u '+ uname+':'+passwd +' -X GET '+localhost+'/api/json/ -o json_outfile/api.json'
	p1 = Popen(shlex.split(cmd))
	print 'reading json file'
	result={}
	#"jobs":[{"_class":"hudson.model.FreeStyleProject","name":"dtf_buildjob","url":"http://localhost:8080/job/dtf_buildjob/","color":"blue"},
	with open('C:\\Python27\\json_outfile\\api.json') as json_file:  
		data = json.load(json_file)
		print data
		for d in data['jobs']:
			#print d
			for i in d['color']:
				if d['color'] == 'blue':
					print d['name']
					success_count+=1
				elif d['color'] == "notbuilt":
					#print d['name']
					nobuilt_count+=1
		print repr("SUCCESS BUILD COUNT : "),success_count
		print "NOBUILT COUNT : ", nobuilt_count
		import re
		#variables = re.search(r'(\d)', success_count)
		#success_count_cleaned = [re.sub(r"[-()\"#/@;:<>{}`+=~|.!?,\w]", "", success_count)]
		print type(success_count)
		print repr(success_count)
		#print success_count.encode('utf-8')
		#print "success_count_cleaned ::", success_count_cleaned
		
		#influxurl='http://localhost:8086/write?db=testdb'
		influxurl=dburl+'/write?db='+dbname
		print "CONNECTION TO DB IS ESTABLISHED !!"
		sc=str(success_count)
		nb=str(nobuilt_count)
		ifDBdata= "jenkins,instance=BLR passed=45,failed=3,notrun=4"
		ifDBdata= "jenkins,instance=BLR passed="+sc+",failed=3,notrun="+nb+""
		#ifDBdata= "jenkinPluginCount,instance=EHV totalpluginInstalled=23"
		#ifDBdata= "jenkinsjobscount,instance=EHV totalbuildcount='9' "
		#select * from jenkinsjobscount where instance='BLR'
		subprocess.call(['curl', '-i', '-n', '-XPOST',influxurl,'--data-binary',ifDBdata])
json_parsing()
"""

print cfgdata.JENKINS_CONFIG.user
localhost = cfgdata.JENKINS_CONFIG.localhost
print localhost
"""

def get_build_info():
	try:
		#uname= cfgdata.JENKINS_CONFIG.user
		
		totalBuildCount = 0
		job_info=[]
		server = Jenkins('http://localhost:8080',username=uname,password=pwd)
		plugins = server.get_plugins()
		noofpluginsinstalled= len(plugins)
		print "TOTAL PLUGINS INSTALLED: ", noofpluginsinstalled

		#list of jobs available
		jobs=server.get_jobs()
		#get the count of each job occurance
		#print list(jobs)
		"""[('dtf_buildjob', <jenkinsapi.job.Job dtf_buildjob>), 
		('dtf_deployjob', <jenkinsapi.job.Job dtf_deployjob>), ('dtf_maven_test', <jenkinsapi.job.Job dtf_maven_test>), 
		('dtf_maven_test1', <jenkinsapi.job.Job dtf_maven_test1>), ('dtf_test', <jenkinsapi.job.Job dtf_test>), 
		('jobformaven', <jenkinsapi.job.Job jobformaven>), ('maven-test', <jenkinsapi.job.Job maven-test>),
		('test', <jenkinsapi.job.Job test>)]
		"""
		
		for job in server.keys():
			builds = server[job].get_build_dict()
			job_info.append(server[job])
			totalBuildCount += len(builds)
		
		#print job_info
		print "TOTAL NO OF BUILDS : ", totalBuildCount
		print "TOTAL NO OF JOBS: ", len(job_info)
	except jenkins.JenkinsException:
		print 'connections issue'
	except jenkins.BadHTTPException:
		print 'broken HTTP response'
	except jenkins.TimeoutException:
		print "socket timeout"
	finally:
		print 'Job Done !!'
		
get_build_info()