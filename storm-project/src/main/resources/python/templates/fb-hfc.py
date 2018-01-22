#!/usr/bin/env python
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from StringIO import StringIO
from colorama import init
from colorama import Fore, Back, Style
from pyvirtualdisplay import Display
import lxml.html
import time
import re
import requests
import argparse
import sys
import os.path
import pickle

init(autoreset=True)

print "-----------------------------------------------------------------------------"
print "          Facebook hidden friends crawler POC - by Shay Priel"
print "-----------------------------------------------------------------------------"
print Fore.BLACK + Style.BRIGHT + "                    .@@@@." + Fore.RESET + "                         .:::,           :;::         "
print Fore.BLACK + Style.BRIGHT + "  lCCCf;            .@@@@.                         " + Fore.RESET + ".:::,           ::::         "
print Fore.BLACK + Style.BRIGHT +  " C@@@@@@t lLLL, tLLL,@@@@iCGL:   :fGGGf,  fLLf:CGf" + Fore.RESET + "..:::,.,,,.,::.  ::::         "
print Fore.BLACK + Style.BRIGHT + "i@@@@@@@@,l@@@; G@@@.@@@@@@@@@, ;@@@@@@@: G@@@@@@@C" + Fore.RESET + ".:::,,::::::::..::::,        "
print Fore.BLACK + Style.BRIGHT +  "C@@@C@@@@l;@@@l G@@C.@@@@@@@@@t.@@@@@@@@G.@@@@@@@@@" + Fore.RESET + ".:::,,::::::::,.::::,        "
print Fore.BLACK + Style.BRIGHT +  "@@@@.i@@@l.@@@f @@@L.@@@@ii@@@C;@@@C.G@@@,@@@@;@@@@" + Fore.RESET + ".:::,,:::, ::::.::::,        "
print Fore.BLACK + Style.BRIGHT +  "@@@@ ;@@@l G@@L.@@@t.@@@@.,@@@Ci@@@l L@@@,@@@G G@@@" + Fore.RESET + ".:::,,:::. ,::: ::::         "
print Fore.BLACK + Style.BRIGHT +  "@@@@ ;@@@l L@@C,@@@;.@@@@.,@@@Gi@@@i f@@@,@@@G G@@@" + Fore.RESET + ".:::,,:::  ,::: ::::         "
print Fore.BLACK + Style.BRIGHT +  "@@@@  ,,,. t@@G;@@@..@@@@.,@@@Ci@@@CfG@@@,@@@G G@@@" + Fore.RESET + ".:::,,:::  :::: ::::         "
print Fore.BLACK + Style.BRIGHT +  "@@@@       i@@Gl@@G .@@@@.,@@@Ci@@@@@@@@@,@@@G G@@@" + Fore.RESET + ".:::,,:::  :::: ::::         "
print Fore.BLACK + Style.BRIGHT +  "@@@@   .   :@@@f@@L .@@@@.,@@@Ci@@@@GGGGG,@@@G ....." + Fore.RESET + ":::,,:::  :::: ::::         "
print Fore.BLACK + Style.BRIGHT +  "@@@@ ;@@@t .@@@G@@t .@@@@.,@@@Ci@@@l      @@@G     " + Fore.RESET + ".:::,,:::  :::: ::::         "
print Fore.BLACK + Style.BRIGHT +  "@@@@ ;@@@f  G@@@@@i .@@@@.,@@@Ci@@@l fGGG,@@@G     " + Fore.RESET + ".:::,,:::  :::: ::::         "
print Fore.BLACK + Style.BRIGHT +  "@@@@ ;@@@f  L@@@@@: .@@@@.,@@@Ci@@@l f@@@,@@@G     " + Fore.RESET + ".:::,,:::  :::: ::::         "
print Fore.BLACK + Style.BRIGHT +  "@@@@,i@@@f  l@@@@@. .@@@@,,@@@Ci@@@t C@@@,@@@G     " + Fore.RESET + ".:::,,:::  :::: ::::         "
print Fore.BLACK + Style.BRIGHT +  "G@@@G@@@@l  ;@@@@C  .@@@@LL@@@L:@@@@t@@@@,@@@G     " + Fore.RESET + ".:::,,:::  :::: ::::"+ Fore.RED +"lilll.   "
print Fore.BLACK + Style.BRIGHT +  "i@@@@@@@@.  ,@@@@f  .@@@@@@@@@i C@@@@@@@L.@@@G     " + Fore.RESET + " :::,,:::. :::: ::::"+ Fore.RED +"llttt.   "
print Fore.BLACK + Style.BRIGHT +  "t@@@@@@i  .;@@@@i  .@@@@f@@@C  ,G@@@@@C  @@@G     " + Fore.RESET + ".:::.,:::  ,::: .:::,"+ Fore.RED +"ltttl.    "
print "-----------------------------------------------------------------------------"
print "Examples:"
print "1. Generates related public profiles:"
print "python fb-hfc.py -username <username>  -password '<password>' \n-query '<graph search query>' -output <output.txt>"
print "2. Exctracting hidden friends:"
print "python fb-hfc.py -username <username>  -password '<password>' \n-target <target> -profilesfile <file.txt> -output <output.txt>"
print "-----------------------------------------------------------------------------"




parser = argparse.ArgumentParser(usage="-h for full usage")
parser.add_argument('-username', dest="username", help='facebook username to login with (e.g. example@example.com)',required=True)
parser.add_argument('-password', dest="password", help='facebook password to login with (e.g. \'password\')',required=True)


args = parser.parse_args()

if args.username is None and args.password is None:
	parser.error("You must give username and password")


def facebook_login(username,password):
	print ("\n\n\nLogin to Facebook...."),
	sys.stdout.flush() 
	url = "http://www.facebook.com"
	driver.get(url)
	elem = driver.find_element_by_id("email")
	elem.send_keys(username)
	elem = driver.find_element_by_id("pass")
	elem.send_keys(password)
	elem.send_keys(Keys.RETURN)
	time.sleep(1)
	html_source = driver.page_source
	if "Please re-enter your password" in html_source or "Incorrect Email" in html_source:
		print Fore.RED + "Incorrect Username or Password"
		driver.close()
		exit()
	else:
		print Fore.GREEN + "Success\n"
	return driver.get_cookies()

	with open('flag.txt', 'w') as myFile:
		#for cookie in cookies:
			#myFile.write(str(cookie).encode('utf8')+"\n")
		for k,v in cookies.iteritems():
			print "dict[%s]="%k,v


username = args.username
password = args.password

display = Display(visible=0, size=(1600, 900))
display.start()

driver = webdriver.Firefox()

cookies = dict()
cookies = facebook_login(username,password)
driver.get("http://www.facebook.com")
pickle.dump( driver.get_cookies() , open("cookies.pkl","wb"))

driver.close()
display.stop()
exit()

