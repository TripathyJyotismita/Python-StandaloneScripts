import jenkinsapi
from jenkinsapi.jenkins import Jenkins
import os, subprocess
import pdb
from influxdb import InfluxDBClient
import request, json, time, httplib2
from json import loads
from urllib2 import urlopen
from requests.auth import HTTPDigestAuth
import json
from subprocess import Popen
import shlex
import datetime
import os
'''
#loginto_jenkins=subprocess.call(["curl", "-v", "-H", "X-Auth-User: myaccount:jyoti_signify1", "-H", "X-Auth-Key: Jyoti@Jenkin1", "http://localhost:8080/login"], shell=False)
#print (loginto_jenkins)


#subprocess.call(["curl", "-v", "-H", "X-Auth-User: myaccount:jyoti_signify1", "-H", "X-Auth-Key: Jyoti@Jenkin1", "http://localhost:8080/login"], shell=False)


#cmd= 'curl -u jyoti_signify1:Jyoti@Jenkin1 -X GET http://localhost:8080/api/json/ -o json_outfile/api.json' 
#p1 = Popen(shlex.split(cmd))

#curl -u jyoti_signify1:Jyoti@Jenkin1 -i -H "Accept: application/json" "http://localhost:8080/api/json/{"name":"dtf_test"}" 
with open('C:\\Python27\\json_outfile\\api.json') as json_file:
		dictionary = json.load(json_file)
		#print dictionary
		print 'SUCCESSFULLY READ DATA FROM JENKINS..!!'
		key='dtf_test'


jobs=['dtf_build', 'dtf_buildjob','dtf_maven_test']
key= 'dtf_build'
def readJson(dictionary):
	if key in dictionary.iteritems():
			for k, v in dictionary.iteritems():
				print 'THIS IS JOBS FOUND: {0}', dictionary[j]



readJson(dictionary)




curdate=str(datetime.datetime.now())
json_body = [
    {
        "measurement": "jenkins_metrics",
        "tags": {
            "jobs": "dtf_test",
        },
        "time": curdate,
        "fields": {
            "value": '0.64'
        }
    }
]
#print json_body

import requests, influxdb
def dbConnection():
	os.environ['http_proxy'] = ""
	os.environ['https_proxy'] = ""
	try:
		client = InfluxDBClient(host='172.17.0.4', port=8086)
		print "CONNECTION TO DB IS ESTABLISHED !!"
		client.create_database('testdb')
		client.write_points(json_body)
		result = client.query('use testdb;')
		print result
		result1 = client.query('show databases;')
		print result1
		print(client.get_list_database())
	except requests.exceptions.ConnectionError as e:
		print e
	except influxdb.exceptions.InfluxDBClientError:
		print "INFLUXDBCLIENTERROR : 403 ERRORCODE"
	
	#client = InfluxDBClient(host='localhost.com', port=8086, username='myuser', password='mypass' ssl=True, verify_ssl=True)
	
#dbConnection()
'''

def datafromdb():
	try:
		#influxurl='http://172.17.0.4:8086/write?db=testdb'
		influxurl='http://localhost:8086/write?db=testdb'
		print "CONNECTION TO DB IS ESTABLISHED !!"
		ifDBdata= "jenkins,instance=BLR passed=12,failed=3,notrun=4"
		#ifDBdata= "jenkins,instance=EHV passed=3,failed=9,notrun=2"
		#ifDBdata= "jenkinsjobscount,instance=BLR totalbuildcount=27,totaljobcount=19"
		ifDBdata= "jenkinsjobscount,instance=EHV totalbuildcount=14,totaljobcount=23"
		#ifDBdata= "jenkinPluginCount,instance=BLR totalpluginInstalled=27"
		#ifDBdata= "jenkinbuildstatus,instance=BLR success=12,nobuilt=40" 
		subprocess.call(['curl', '-i', '-n', '-XPOST',influxurl,'--data-binary',ifDBdata])
		print 'SUCCESSFULLY LOADED TO DB !!'
	except:
		print 'Unable to estabilish connection'
	finally:
		print 'insert to db is done!!'

datafromdb()


