import os,sys,re,json,datetime
from mail import send

def test(path,sbBuildNumber,library,changedVer,comp,ch="false"):
        if ch=="false":
                if os.path.isfile("verification_summary.txt"):
                        os.system("rm verification_summary.txt")

                ver=open("verification_summary.txt","w+")
                ver.write("VERIFICATION SUMMARY".center(60,'='))
                ver.write("\n\nOSS".ljust(25)+" : "+library+"\n")
                ver.write("SB LINK".ljust(25)+" : https://buildweb.eng.vmware.com/sb/"+sbBuildNumber+"/\n")
                ver.write("VERSION EXPECTED".ljust(25)+" : "+changedVer+"\n")
		ver.write("OBJECT PATH TO TEST".ljust(25)+" : "+comp+"\n")
                ver.close()

                verify(path,sbBuildNumber,library,changedVer,comp,False)

        elif ch=="true":
                verify(path,sbBuildNumber,library,changedVer,comp,True)

def verify(path,sbBuildNumber,library,changedVer,comp,flag):
        if os.path.isfile(path+'/nimbus-vc-upgrade-and-test.json'):
                print ('')
                print ('#'*100)
                print ('TESTING AND VERIFICATION IN PROGRESS.......')

                if flag:
                        rep=open("summary.txt","a")

                sourcejson=json.load(open(path+'/nimbus-vc-upgrade-and-test.json'))
                data=sourcejson['sourcevc1']
                ip=data['ip']
                user=data['adminUser']
                password=data['adminPassword']
                hashpass=password[:1]+'*'*(len(password)-2)+password[-1:]
                f1=open('verification_summary.txt','a')
                a=path.split('_')
                name="null"
                for val in a:
                        if "Cloud" in val:
                                name=val
                        elif "Ciswin" in val:
                                name=val
                f1.write("\nVERIFICATION FOR "+name.upper()+" :\n")
                if 'Cloud' in path:
                        print ("LOGGING INTO "+user+"@"+ip)
			os.system("sshpass -p "+password+" ssh "+user+"@"+ip+" grep '"+changedVer+"' "+comp+"> ver.txt")
        		f=open('ver.txt','r')
        		ver=f.read()
			f.close()
			os.system("rm ver.txt")
        		print ver
        		if 'matches' in ver:
				print("VERSION VERIFICATION IS SUCCESSFUL")
                                if flag:
	                                rep.write("VERSION VERIFICATION".ljust(25)+" :  SUCCESSFUL\n")
                                f1.write("VERSION VERIFICATION".ljust(25)+" : SUCCESSFUL\n")

        		else:
                                print("VERSION VERIFICATION FAILED ")
                                if flag:
                                	rep.write("VERSION VERIFICATION".ljust(25)+" :  FAILED\n")
                                f1.write("VERSION VERIFICATION".ljust(25)+" : FAILURE\n")
                        print("\n")

                else:
                        print ("FOR CISWIN DEPLOYMENT")
                        print ("IP".ljust(14)+"- "+ip)
                        print ("USERNAME".ljust(14)+"- "+user)
                        print ("PASSWORD".ljust(14)+"- "+hashpass)
                        print ("VERIFICATION FOR CISWIN SHOULD BE DONE MANUALLY")
                        if flag:
                                rep.write("VERSION VERIFICATION".ljust(25)+" : TO BE DONE MANUALLY\n")
                                rep.write ("IP".ljust(25)+" : "+ip+"\n")
                                rep.write ("USERNAME".ljust(25)+" : "+user+"\n")
                                rep.write ("PASSWORD".ljust(25)+" : "+hashpass+"\n")

                        f1.write("VERSION VERIFICATION".ljust(25)+" : NA\n")
                        f1.write("NOTE".ljust(25)+" : VERIFICATION TO BE DONE MANUALLY\n")
                        f1.write ("IP".ljust(25)+" : "+ip+"\n")
                        f1.write ("USERNAME".ljust(25)+" : "+user+"\n")
                        f1.write ("PASSWORD".ljust(25)+" : "+hashpass+"\n")
                if flag:
                        rep.write("LOGFILES PATH".ljust(25)+" : "+path+"\n\n")
                        rep.close()
                if not flag:
                        f1.write("END OF REPORT".center(60,'='))
                f1.close()
                if not flag:
                        notify_verification(library)
                print("TESTING AND VERIFICATION COMPLETED\n")

                print ("TIMESTAMP - "+str(datetime.datetime.now())+"\n")
        else:
                print ("ERROR : PATH PROVIDED IS INCORRECT OR FILE NOT FOUND")


def notify_verification(lib):
        ver=open('verification_summary.txt','r')
        line = ver.read()
        ver.close()
        send(line,lib+"| Verification Report",0," ")


def main():
        try:
                logfiles=sys.argv[1]
                sbBuildNumber=sys.argv[2]
                library=sys.argv[3]
                changedVer=sys.argv[4]
		comp=sys.argv[5]
                if len(sys.argv)==6:
                        test(logfiles,sbBuildNumber,library,changedVer,comp)
                elif len(sys.argv)==7:
                        test(logfiles,sbBuildNumber,library,changedVer,comp,sys.argv[6])
                else:
                        raise IndexError

        except IndexError:
                print("ERROR : THE ARGUMENTS FOR THE SCRIPT ARE IN THE ORDER :<PATH_FOR_LOGFILES>  <SB_BUILD_NUMBER> <OSS> <EXPECTED_VERSION> <COMPONENT>")


if __name__=="__main__":
        main()

