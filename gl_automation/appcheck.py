import subprocess,json,requests,urllib,re,time
import os ,sys,re,fnmatch, httplib2, requests,subprocess,time
import requests.packages.urllib3
from requests.auth import HTTPBasicAuth
requests.packages.urllib3.disable_warnings()
from time import sleep
from automationUtil import cfgdata
try:
    import simplejson as json
except ImportError:
    import json
client = httplib2.Http()
class HttpResponseError(Exception):
        def __init__(self,status):
                if status==400:
                        error="ERROR : 400 Bad Request"
                elif status==404:
                        error="ERROR : 404 Resource not found"
                elif status==405:
                        error="ERROR : 405 Method not allowed"
                else:
                        error="ERROR : 500 Service is unavailable or internal server error."

                self.value=error

        def __str__(self):
                return(repr(self.value))
def get_resource(url, verbose=True):
        try:
                response,content = client.request(url)
                status = int(response['status'])
                data = json.loads(content)
                if status != 200:
                        raise HttpResponseError(status)
        except HttpResponseError as error:
                print ("HTTP_RESPONSE_ERROR : "+error.value)
        except httplib2.ServerNotFoundError,e:
                print("ERROR : "+str(e))
                sys.exit(0)
        return status,data
#To get the json values and call the appcheck.sh for triggering
dbcpassword=cfgdata['password']['dbc']
username=cfgdata['user']
pollingTime=cfgdata['pollingTimeInMin']['appcheckStatus']
pollingTime2=cfgdata['pollingTimeInMin']['verificationStatus']
def jsonparse(sb_num):
        buildAndtype=sb_num.split('-')
        try:
             buildno=buildAndtype[1]
             buildtype=buildAndtype[0]
        except:
                print "Build type or Build no. not proper"
                sys.exit()
        buildurl="http://buildapi.eng.vmware.com//"+buildtype+"/deliverable/?build="+buildno
        status,content=get_resource(buildurl)
        try:
                apiResponseCount=content['_page_count']
        except:
                print "Build no. or Build type  not specified"
                sys.exit()
        if apiResponseCount==100:
           getVersionUrl="http://buildapi.eng.vmware.com/"+buildtype+"/build/?id="+buildno+"&_format=json"
           statusVersion,contentVersion=get_resource(getVersionUrl)
        else:
          print "There are no deliverables for the specifyed build nummber"
	  quit()
        allData=contentVersion['_list']
        for data1 in allData:
                          version=data1['version']
        try:
            vim="http://build-squid.eng.vmware.com/build/mts/release/"+sb_num+"/publish/vim-iso/VMware-VIM-all-"+version+"-"+buildno+".iso"
            vcsa="http://build-squid.eng.vmware.com/build/mts/release/"+sb_num+"/publish/vcsa-iso/VMware-VCSA-all-"+version+"-"+buildno+".iso"
            print "Appcheck Started........"
            subprocess.call(["./appcheck.sh %s %s %s %s"%(vim,vcsa,dbcpassword,username)],shell=True)
	    sleep(pollingTime)
        except:
                print "Build is not found"
                sys.exit()
        return vim,vcsa
#To Trigger the Appcheck when run individually
def tri(sb_num):
        jsonparse(sb_num)
        try:
                        print "Triggering appcheck for vim-iso:"
                        jsonfile=json.load(open('vim.json'))
			resultUrl=jsonfile['results']
                        print resultUrl['report_url']
			removevim="rm vim.json"
			os.system(removevim)
                        print "Triggering appcheck for vcsa-iso:"
                        jsonfile1=json.load(open('vcsa.json'))
                        resultUrl1=jsonfile1['results']
                        print resultUrl1['report_url']
			removevcsa="rm vcsa.json"
                        os.system(removevcsa)
        except:
                        print "Checking for status"
			sleep(pollingTime2)
                        print "Already being Scanned"
                        tri(sb_num)
#Function to Trigger the Appcheck from the main program
def trig(sb_num,logger):
	vim,vcsa=jsonparse(sb_num)
        logger.write("\nAppcheck Started.......\n")
        logger.write("VIM iso : "+vim+"\n")
        logger.write("VCSA iso : "+vcsa+"\n")
#To check for the affected files from the object path
def Componentcheck(component,version,sb_num):
                        print "                                                            Affected Files"
                        print "                                                            -------- -----"
                        print_row('Filename','Object Path','')
                        print_row('--------','-----------','')
                        try:
                            for objectPath in component['extended-objects']:
                                                    Path=objectPath['fullpath'][1]
                                                    Fullpath=Path.encode('ascii','ignore')
                                                    if Fullpath.startswith('vCenter'):
                                                                            print_row(objectPath['name'],Fullpath,'')
                                                                            filename_with_extension = os.path.basename(Fullpath)
                                                                            filename, extension = os.path.splitext(filename_with_extension)
                                                                            deliverableUrl="http://build-squid.eng.vmware.com/build/mts/release/sb-"+sb_num+"/publish/vcenter/"+filename
                                                                            content=requests.head(deliverableUrl).headers.get('content-length', None)
                                                                            size=int(content)
                                                                            convertMb=1048576
                                                                            inmb=size/convertMb
                                                                            print "Filesize:-"+str(inmb)+"MB Downloading"
                                                                            commandDownload="wget "+deliverableUrl+"&> /dev/null"
                                                                            os.system(commandDownload)
                                                                            removeFile="rm "+filename
                                                                            commandGrep="grep -F --color "+version+"  "+filename
                                                                            os.system(commandGrep)
                                                                            os.system(removeFile)
                                                    else:
                                                        print"No File and Object for Particular library"
                        except:
                            print "No Deliverables found for the build no."
#Version verification of a particular library
def appcheck(check,version,sb_num,libname,logger):
        try:
                report_summary=open("summary.txt","a")
                if check=="VIM":
                        print check
                        jsonfile=json.load(open('vim.json'))
                elif check=="VCSA":
                        print check
                        jsonfile=json.load(open('vcsa.json'))
                resultUrl=jsonfile['results']
                print resultUrl['report_url']
                nummber=re.findall('\d+',resultUrl['report_url'])
                checkNummber=str(nummber[0])
                appcheckUrl="https://appcheck.eng.vmware.com/api/product/"+checkNummber+"/"
                logger.write("Appcheck url : "+appcheckUrl+"\n")
                decryptPassword=os.popen("echo %s|openssl enc -aes-128-cbc -a -d -salt -pass pass:121"%(dbcpassword)).read()
                password=decryptPassword.strip()
                try:
                        os.system("wget "+appcheckUrl+" --user="+username+" --password="+password+" -O appcheck.json -o /dev/null")
                        appcheckJson=json.load(open('appcheck.json'))
                except:
                         print "Appcheck Server Down"
                         quit()
		removeJson="rm appcheck.json"
		removecheck="rm "+check.lower()+".json"
                resultJson=appcheckJson['results']
                status=resultJson['status']
                components=resultJson['components']
                if status=="R":
                        print "Status : "+status
                        try:
                                for comp in components:
                                        if comp['lib']==libname:
                                                print "Library Found"+comp['lib']
                                                if comp['version']==version:
                                                        print "APPCHECK : Version Matched - "+comp['version']
                                                        k="APPCHECK : Version Matched - "+comp['version']
                                                        report_summary.write(check+" : https://appcheck.eng.vmware.com/products/"+checkNummber+"/\n")
                                                        report_summary.write("VERSION MATCHED FOR "+check.upper()+"\n")
                                                        logger.write("INFO : VERSION MATCHED FOR "+check.upper()+"\n")
				
							os.system(removeJson)
                                                        break
                                                elif comp['version'] is None:
					                  print "The version is null for the library"
							  buildnum=sb_num.split('-')
                                                 	  builno=buildnum[1]
                                                          print"Component Check.."
                                                          Componentcheck(comp,version,buildno)
                                                else:
                                                        report_summary.write(check+" : https://appcheck.eng.vmware.com/products/"+checkNummber+"/\n")
                                                        report_summary.write("VERSION MATCH FAILED FOR "+check.upper()+"\n")
                                                        logger.write("FAILED : VERSION MATCH FAILED FOR "+check.upper()+"\n")
                                                        print "Version Field is Invalid or Null"
                                                        print "APPCHECK : Version verification Failed - "+version
                        except:
				sleep(pollingTime2)
                        	appcheck(check,version,sb_num,libname,logger)
                else:
                        print "Analysis Failed"
                report_summary.close()
        except:
                print "Error while reading json file : "+check
	os.system(removecheck)
def print_row(filename, status, file_type):
    print "%-85s %20s %15s" % (filename, status, file_type)
#Appcheck verification when run individually
def appc(url,libname,version,buildno):
                decryptPassword=os.popen("echo %s|openssl enc -aes-128-cbc -a -d -salt -pass pass:121"%(dbcpassword)).read()
                password=decryptPassword.strip()
                try:
                      os.system("wget "+url+" --user="+username+" --password="+password+" -O appcheck.json -o /dev/null")
                      jsonfile=json.load(open('appcheck.json'))
                except:
                      print "Invalid Link Or Appcheck Server Down"
                      quit()
                appcheck=jsonfile['results']
                status=appcheck['status']
		removeJson="rm appcheck.json"
                components=appcheck['components']
                if status=="R":
                     print "Status : "+status
                     try:
                            for comp in components:
                                if comp['lib']==libname:
                                        print "Library Found:-"+comp['lib']
                                        if comp['version']==version:
                                                print "APPCHECK: Version Matched-"+comp['version']
                                                k="APPCHiECK : Version Matched - "+comp['version']
						print "                                                        Affected Files"
                        			print "                                                        -------- -----"
                        			print_row('Filename','Object Path','')
                        			print_row('--------','-----------','')
						for objectPath in comp['extended-objects']:
                                	              		Path=objectPath['fullpath'][1]
                                        	          	Fullpath=Path.encode('ascii','ignore')
                                                	   	if Fullpath.startswith('vCenter'):
                                                        	                    print_row(objectPath['name'],Fullpath,'')
						os.system(removeJson)
                                                break
                                        elif comp['version'] is None:
							print "Version is None"
                                                       
                     except:
                                print "Version Field is Invalid or Null"
                                print "APPCHECK : Version verification Failed - "+version
				os.system(removeJson)
                elif status=="B":
                        print "File is  being scanned....: "
                        sleep(pollingTime2)
                        appc(url,libname,version,buildno)
                else:
                        print "Analysis Failed"
#For alignment and printig
def print_row2(filename, status, file_type):
    print "%-15s %5s %15s" % (filename, status,file_type)
#For taking options from the user
def choice():
                 print('1. Triggering  the Appcheck\n')
                 print('2. Appcheck verification\n')
                 print('3. EXIT\n')
                 choice =int(input('Enter your choice:'))
                 if (choice == 1):
                        buildno=raw_input('Build No-Build Type:-eg:sb-13083539\n')
                        tri(buildno)
                 elif (choice == 2):
                                 buildno=raw_input('Enter Build no:-\n')
                                 url=raw_input('Appcheck Link:-\n')
                                 libname=raw_input('Library Name:-\n')
                                 version=raw_input('Library Version to Verify:-\n')
                                 if libname=="":
                                                print "Library is NUll"
                                                quit()
                                 appc(url,libname,version,buildno)
                 elif choice==3:
                        quit()
                 else:
                        print('Invalid choice')
			choice()
#For help options
def hlp():
                print_row2('Usage:\n','','')
                print_row2('','python appcheck2.py','')
                print_row2('Summary:\n','','')
                print_row2('Appcheck script is used to Trigger and verify the version from Appcheck Api.\nAppcheck trigger is used  to trigger for both vim and vcsa parallelly and gets the URL\nof the deliverable being scanned ,then  verifys the version.\nIf still being scanned it waits for 25 minutes ,the wait loop continues until the status is R.\nIf the status is F safely exit from the program.\n','','')
                print_row2('Options:\n','','')
                print_row2('','- Triggering  the Appcheck','')
                print_row2('','  1.Enter the  build no. with sb or ob eg:-sb-1325626','')
                print_row2('','- Verification of the version from Appcheck','')
                print_row2('','  1.Enter the Appcheck  url for appcheck velsrification eg:-https://appcheck.eng.vmware.com/api/product/29945/','')
                print_row2('','  2.Enter the build no.','')
                print_row2('','  3.Enter the Oss Version needs to be verifyed eg:-3.20.1','')
                print_row2('','- For Appcheck Trigger and Verification','')
                print_row2('','  1.Enter the url,build no.,version,lib to verify and trigger Appcheck\n','')
if __name__ == '__main__':
        try:
            ars=sys.argv[1]
            if ars=="-h" or ars=="-H" or ars=="--help" or ars=="--HELP":
                         hlp()
        except:
            choice()
