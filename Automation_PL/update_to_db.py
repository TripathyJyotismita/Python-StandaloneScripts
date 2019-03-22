#runs only on no proxy
#JOBS NOT TRIGGERED:  0
#JOBS DISABLED:  0
#JOBS SUCCESS:  3
#FAILED JOBS:  0

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
token = cfgdata.token
localhost = cfgdata.localhost
dburl = cfgdata.dburl
dbname=cfgdata.dbname

def json_parsing():
	success_count = 0
	nobuilt_count = 0
	disabled_count = 0
	failed_count = 0
	#cmd= 'curl -u '+ uname+':'+token +' -X GET '+localhost+'/api/json/ -o json_outfile/api.json'
	cmd = 'curl -u signifyjenkins:signify -X GET '+localhost+'/api/json/ -o json_outfile/api.json'
	p1 = Popen(shlex.split(cmd))

	#with open('C:\\Python27\\json_outfile\\test-json-file.json') as json_file:  
	#	data = json.load(json_file)
	with open('C:\\Python27\\json_outfile\\api.json') as json_file:  
		data = json.load(json_file)
		#print data
		""" sample input
		data=
		{"jobs": [
		{
		"_class": "hudson.model.FreeStyleProject",
		"name": "01-ARM9-UnitTests",
		"url": "https://jenkins-blr.rndit.intra.lighting.com/job/Amplight/job/Amplight_Gen2/job/01-ARM9-UnitTests/",
		"color": "red"
		}}"""
		
		nobuild_jobs=[]
		total_job_count=0
		
		for items in data["jobs"]: # data is a dict with value as a list whose elementsare dict
			for lie in items: #lie=elements in items, here item is a list
				if lie == "color":
					#print items["color"]
					if items["color"] == "notbuilt":
						#print items["color"]
						nobuilt_count+=1
					elif items["color"] == "disabled":
						#print items["color"]
						disabled_count+=1
					elif items["color"] == "blue":
						success_count+=1
					elif items["color"] == "red":
						failed_count+=1
				if lie == "name":
					print items["name"]
					total_job_count=+1
					
					
		print "JOBS NOT TRIGGERED: ",nobuilt_count
		print "JOBS DISABLED: ",disabled_count
		print "JOBS SUCCESS: ",success_count
		print "FAILED JOBS: ", failed_count
		#print "TOTAL JOBS AVAILABLE: ",total_job_count

		#influxurl='http://localhost:8086/write?db=testdb'
		influxurl=dburl+'/write?db='+dbname
		print "CONNECTION TO DB IS ESTABLISHED !!"
		sc=str(success_count)
		nb=str(nobuilt_count)
		fb=str(failed_count)
		#ifDBdata= "jenkins,instance=BLR passed=45,failed=3,notrun=4"
		ifDBdata= "jenkins,instance=BLR passed="+sc+",failed="+fb+",notrun="+nb+""
		#ifDBdata= "jenkinPluginCount,instance=EHV totalpluginInstalled=23"
		#ifDBdata= "jenkinsjobscount,instance=EHV totalbuildcount='9' "
		#select * from jenkinsjobscount where instance='BLR'
		subprocess.call(['curl', '-i', '-n', '-XPOST',influxurl,'--data-binary',ifDBdata])

json_parsing()