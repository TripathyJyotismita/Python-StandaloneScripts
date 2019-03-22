import time,shutil,os, sys, subprocess,json,re
from mail import send
from automationUtil import cfgdata
global single
global lib
lib=""
single=True
choices=[]
def CloudVm(sb_num,timestamp,logger):
        print "CloudVm Fresh Install in progess..........\n"
	logger.write("CloudVm Fresh Install in progess..........\n")
	logger.write("/mts/git/bin/nimbus-vc-upgrade-and-test --sourceVCBuild "+sb_num+" --dbType embedded --upgradeTool none --resultsDir=/tmp/logfiles_CloudVm_"+timestamp+" --> outputfile_CloudVm.out\n\n")
        subprocess.Popen(["/mts/git/bin/nimbus-vc-upgrade-and-test --sourceVCBuild "+sb_num+" --dbType embedded --upgradeTool none --resultsDir=/tmp/logfiles_CloudVm_"+timestamp+" &>>outputfile_CloudVm.out"],shell=True)

def Ciswin(sb_num,timestamp,logger):
        print "Ciswin Fresh Install in progess..........\n"
	logger.write("Ciswin Fresh Install in progess..........\n")
	logger.write("/mts/git/bin/nimbus-vc-upgrade-and-test --sourceVCBuild "+sb_num+" --sourceOS windows --dbType embedded --topology allInOne --upgradeTool none --resultsDir=/tmp/logfiles_Ciswin_"+timestamp+" --> outputfile_Ciswin.out\n\n")
        subprocess.Popen(["/mts/git/bin/nimbus-vc-upgrade-and-test --sourceVCBuild "+sb_num+" --sourceOS windows --dbType embedded --topology allInOne --upgradeTool none --resultsDir=/tmp/logfiles_Ciswin_"+timestamp+" &>>outputfile_Ciswin.out"],shell=True)

def MinorCloudVm(sb_num,timestamp,logger):
        print "CloudVm Minor Upgrade in progess..........\n"
	logger.write("CloudVm Minor Upgrade in progess..........\n")
	logger.write("/mts/git/bin/nimbus-vc-upgrade-and-test --sourceVCBuild ob-"+str(cfgdata['sourceBuildForDeployment']['cloudvm']['minor']['id'])+" --targetVCBuild "+sb_num+" --upgradeTool b2b --upgradeSourceType iso --dbType embedded --topology allInOne --resultsDir=/tmp/logfiles_MinorCloudVm_"+timestamp+" -->outputfile_MinorCloudVm.out\n\n")
        subprocess.Popen(["/mts/git/bin/nimbus-vc-upgrade-and-test --sourceVCBuild ob-"+str(cfgdata['sourceBuildForDeployment']['cloudvm']['minor']['id'])+" --targetVCBuild "+sb_num+" --upgradeTool b2b --upgradeSourceType iso --dbType embedded --topology allInOne --resultsDir=/tmp/logfiles_MinorCloudVm_"+timestamp+" &>>outputfile_MinorCloudVm.out"],shell=True)

def MajorCloudVm(sb_num,timestamp,logger):
	logger.write("CloudVm Major Upgrade in progess..........\n")
	logger.write("/mts/git/bin/nimbus-vc-upgrade-and-test --sourceVCBuild ob-"+str(cfgdata['sourceBuildForDeployment']['cloudvm']['major']['id'])+" --sourceVCVersion "+str(cfgdata['sourceBuildForDeployment']['cloudvm']['major']['sourceVersion'])+" --targetVCBuild "+sb_num+" --dbType embedded --topology allInOne --resultsDir=/tmp/logfiles_MajorCloudVm_"+timestamp+" --> outputfile_MajorCloudVm.out\n\n")
        print "CloudVm Major Upgrade in progess..........\n"
        subprocess.Popen(["/mts/git/bin/nimbus-vc-upgrade-and-test --sourceVCBuild ob-"+str(cfgdata['sourceBuildForDeployment']['cloudvm']['major']['id'])+" --sourceVCVersion "+str(cfgdata['sourceBuildForDeployment']['cloudvm']['major']['sourceVersion'])+" --targetVCBuild "+sb_num+" --dbType embedded --topology allInOne --resultsDir=/tmp/logfiles_MajorCloudVm_"+timestamp+" &>>outputfile_MajorCloudVm.out"],shell=True)

def MinorCiswinVm(sb_num,timestamp,logger):
	logger.write("Ciswin Minor Upgrade in progess..........\n")
	logger.write("/mts/git/bin/nimbus-vc-upgrade-and-test --sourceVCBuild ob-"+str(cfgdata['sourceBuildForDeployment']['ciswin']['minor']['id'])+" --sourceOS windows --targetVCBuild "+sb_num+" --targetOS windows --dbType embedded --topology allInOne --resultsDir=/tmp/logfiles_MinorCiswinVm_"+timestamp+" -->outputfile_MinorCiswinVm.out\n\n")
        print "Ciswin Minor Upgrade in progess..........\n"
        subprocess.Popen(["/mts/git/bin/nimbus-vc-upgrade-and-test --sourceVCBuild ob-"+str(cfgdata['sourceBuildForDeployment']['ciswin']['minor']['id'])+" --sourceOS windows --targetVCBuild "+sb_num+" --targetOS windows --dbType embedded --topology allInOne --resultsDir=/tmp/logfiles_MinorCiswinVm_"+timestamp+" &>>outputfile_MinorCiswinVm.out"],shell=True)

def MajorCiswinVm(sb_num,timestamp,logger):
	logger.write("Ciswin Major Upgrade in progess..........\n")
	logger.write("/mts/git/bin/nimbus-vc-upgrade-and-test --sourceVCBuild ob-"+str(cfgdata['sourceBuildForDeployment']['ciswin']['major']['id'])+" --sourceVCVersion "+str(cfgdata['sourceBuildForDeployment']['ciswin']['major']['sourceVersion'])+" --targetVCBuild "+sb_num+" --targetOS windows --dbType embedded --topology allInOne --resultsDir=/tmp/logfiles_MajorCiswinVm_"+timestamp+" -->outputfile_MajorCiswinVm.out\n\n")
        print "Ciswin Major Upgrade in progess..........\n"
        subprocess.Popen(["/mts/git/bin/nimbus-vc-upgrade-and-test --sourceVCBuild ob-"+str(cfgdata['sourceBuildForDeployment']['ciswin']['major']['id'])+" --sourceVCVersion "+str(cfgdata['sourceBuildForDeployment']['ciswin']['major']['sourceVersion'])+" --targetVCBuild "+sb_num+" --targetOS windows --dbType embedded --topology allInOne --resultsDir=/tmp/logfiles_MajorCiswinVm_"+timestamp+" &>>outputfile_MajorCiswinVm.out"],shell=True)


def select(sb_num, ch="false",timestamp=""):
	if timestamp=="":
	        t = time.localtime()
	        timestamp = time.strftime('%b%d%Y_%H%M', t)
	
	
	optionsList = {
        	1 : "1. CloudVmfresh Install",
                2 : "2. Ciswin Fresh Install",
                3 : "3. Minor CloudVm Upgrade",
                4 : "4. Major CloudVm Upgrade",
                5 : "5. Minor Ciswin Upgrade",
                6 : "6. Major Ciswin Upgrade"
        }
	if ch=="true":
		global lib
		global single
		single=False
		cont=json.load(open('config1.json'))
		lib=cont['oss']
		print "\nDEPLOYMENT TAKING PLACE FOR :\n"
        	logger=open("output.log","a")
		logger.write("\nDEPLOYMENT TAKING PLACE FOR :\n")		
        	for key, value in optionsList.iteritems():
        		print value
			logger.write(value+"\n")
		num=[1,2,3,4,5,6]
		deploy(num,sb_num,timestamp,logger)

	elif ch=="false":
                logger=open("output_deploy.log","w+")
                logger.write("\nDEPLOYMENT TAKING PLACE FOR :\n")

		while(True):
			print ("\nPlease select the deployment from  below options : ")
        		print ("e.g  1 2 4")
        		print ("")
		
        		for key, value in optionsList.iteritems():
                		print value	

			print('Enter the Options')
			try:
        			num=map(int,raw_input().split())
				global choices
				choices=num
				if validation(num):
					print ("Below are the selected options")

        				for val in num:
                				print (optionsList.get(val))

        				print ("Proceed for deployment? (y/n)")

        				proceed=raw_input()
        				if proceed=="y":
						for key, value in optionsList.iteritems():
							logger.write(value+"\n")
                				print ("DEPLOYMENT STARTED".center(80,'='))
						deploy(num,sb_num,timestamp,logger)
						break
        				elif proceed=="n":
                				print ("PLEASE SELECT OPTIONS AGAIN")
						print ("#"*80)
						print("")
					else:
						print ("ERROR : INVALID INPUT")
						
			except Exception, e:
				print "ERROR INVALID INPUT"
				print str(e)
	else:
		print "ERROR : INVALID ARGUMENT PASSED FOR THE SCRIPT"


def validation(choice):
	if len(choice)==0:
		print "ERROR : NO INPUTS SELECTED"
		return False
	if len(choice)>6:
        	print "ERROR : MORE INPUTS SELECTED"
		return False
	for val in choice:
		if val>6 or val<1:
			print "ERROR : INVALID INPUT"
			return False
		elif type(val)!= int:
			print "ERROR : ONLY INTEGERS ARE ALLOWED"
			return False
		elif choice.count(val)>1:
			print "ERROR : REPEATED ENTRIES" 
			return False
		else:
			pass
	return True


def deploy(dep,sb_num,timestamp,logger):	
	try:
		if os.path.isfile("choices.txt"):
			os.system("rm choices.txt")
	except:
		pass
        f=open("choices.txt","a+")
        for b in dep:
                f.write("%d "%b)
        f.close()


	options = {1 : CloudVm,
           2 : Ciswin,
           3 : MinorCloudVm,
           4 : MajorCloudVm,
           5 : MinorCiswinVm,
           6 : MajorCiswinVm
        }
	
	for val in dep:
		options[val](sb_num,timestamp,logger)

	poll_deploy(dep,timestamp,logger)


def init_report(sb_int):
	if os.path.isfile("dep_report.txt"):
		os.system("rm dep_report.txt")
        depReport1=open("dep_report.txt","w+")
        depReport1.write("DEPLOYMENT SUMMARY".center(60,'=')+"\n\n")
        depReport1.write("SB LINK : https://buildweb.eng.vmware.com/sb/"+str(sb_int)+"/\n\n")
        depReport1.write("DEPLOYMENT STATUS :\n\n")
	depReport1.close()

def notify_deploy():
        f4 = open("dep_report.txt","r")
        line = f4.read()
        f4.close()
	if single:
		send(line,"Deployment Report",0," ")
	else:
        	send(line,lib+" | Deployment Report",0," ")


def poll_deploy(dep,timestamp,logger):
        names = {
                1 : "CLOUDVM FRESH INSTALL",
                2 : "CISWIN FRESH INSTALL",
                3 : "MINOR CLOUDVM UPGRADE",
                4 : "MAJOR CLOUDVM UPGRADE",
                5 : "MINOR CISWIN UPGRADE",
                6 : "MAJOR CISWIN UPGRADE"
        }

        options = {1 : "CloudVm",
                2 : "Ciswin",
                3 : "MinorCloudVm",
                4 : "MajorCloudVm",
                5 : "MinorCiswinVm",
                6 : "MajorCiswinVm"
        }


        flag=True
        totaltime=0
        dep1=[]
        count=0
	logger.write("Polling started for deployment status\n")
	depReport=open("dep_report.txt","a")
	waittime=cfgdata['pollingTimeInMin']['deploymentStatus']*60
        while True:
                for val in dep:
                        if val in dep1:
                                continue
                        folder="/tmp/logfiles_"+options.get(val)+"_"+timestamp

                        if os.path.isfile(folder+"/nimbus-vc-upgrade-and-test-test-results.json"):
                                time.sleep(120)
                                if os.stat(folder+"/nimbus-vc-upgrade-and-test-test-results.json").st_size!=0:

                                        jsonresult=json.load(open(folder+"/nimbus-vc-upgrade-and-test-test-results.json"))

                                        for res in jsonresult:
                                                if res['pass']!=True:
 							print ("DEPLOYMENT FAILED FOR "+options.get(val))
                                                        logger.write("FAILED : DEPLOYMENT FAILED FOR "+options.get(val)+"\n")
                                                        flag=False
                                                        break
                                        if flag:
                                                depReport.write(names.get(val).ljust(25)+" : SUCCESFUL\n\n")
						logger.write(names.get(val).ljust(25)+" : SUCCESFUL\n")
                                                print ("DEPLOYMENT IS SUCCESSFUL FOR "+options.get(val))
                                        else:
                                                depReport.write(names.get(val).ljust(25)+" : FAILURE\n")
						depReport.write("FAILURE LOGS".ljust(25)+" : "+folder+"/nimbus-vc-upgrade-and-test-test-results.json\n\n")
						logger.write(names.get(val).ljust(25)+" : FAILURE\n")
						logger.write("FAILURE LOGS".ljust(25)+" : "+folder+"/nimbus-vc-upgrade-and-test-test-results.json\n")


                                else:
                                        print("DEPLOYMENT FAILED FOR "+options.get(val)+"\n")

                                        depReport.write(names.get(val).ljust(25)+" : "+"FAILURE\n")
					depReport.write("FAILURE LOGS".ljust(25)+" : "+folder+"/nimbus-vc-upgrade-and-test-test-results.json\n\n")
					logger.write(names.get(val).ljust(25)+" : FAILURE\n")
                                        logger.write("FAILURE LOGS".ljust(25)+" : "+folder+"/nimbus-vc-upgrade-and-test-test-results.json\n")

                                dep1.append(val)
                        flag=True

                totaltime=totaltime+18
                if len(dep)==len(dep1):
                        break
                if totaltime==234:
                        for val in dep:
                                if val in dep1:
                                        continue
                                print "DEPLOYMENT IS FAILED FOR "+options.get(val)
                                depReport.write(names.get(val).ljust(25)+" : "+"FAILURE\n")
				depReport.write("FAILURE LOGS".ljust(25)+" : "+folder+"/nimbus-vc-upgrade-and-test-test-results.json\n\n")
                                logger.write(names.get(val).ljust(25)+" : FAILURE\n")
                                logger.write("FAILURE LOGS".ljust(25)+" : "+folder+"/nimbus-vc-upgrade-and-test-test-results.json\n")
                        break
                time.sleep(waittime)
	depReport.write("END OF REPORT".center(60,'='))	
        depReport.close()
	logger.write("\nDeployment completed\n\n")
	logger.close()
	notify_deploy()

def main():
	try:
		sb_num=sys.argv[1]
		sb_int=int(re.search(r'\d+', sb_num).group())
		init_report(sb_int)
		x=int(sb_int)
		if len(sys.argv)==2:	
			select(sb_num)
		elif len(sys.argv)==3:
			select(sb_num, sys.argv[2])
                elif len(sys.argv)==4:
                        select(sb_num, sys.argv[2],sys.argv[3])
		else:
			raise IndexError
	except IndexError:
        	print("ERROR : THE ARGUMENT FOR THE SCRIPT IS <SB_BUILD_NUMBER>")
	except ValueError:
        	print("ERROR : INVALID BUILD NUMBER - "+sb_num)

		
if __name__=="__main__":
        main()
