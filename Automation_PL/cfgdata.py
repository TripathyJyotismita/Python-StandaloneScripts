import os, json
##"ApiToken" : "11c76e015a70b50c46f601cde9dce95eae"

with open("C:\Users\\600038198\\Desktop\\Automation_PL\\config.json","r") as file:
	cf=json.load(file)
	print cf
	
user = cf['user']
passwd = cf['passwd']
token = cf['apitoken']
localhost = cf['localhost']
dbname = cf['dbname']
dburl = cf['dburl']
