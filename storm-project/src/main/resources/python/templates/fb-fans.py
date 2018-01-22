#!/usr/bin/env python
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from StringIO import StringIO
from colorama import init
from colorama import Fore, Back, Style
from pyvirtualdisplay import Display
from lxml import etree
import lxml.html
import time
import re
import requests
import argparse
import sys
import os.path
import pickle
import codecs
#from common import *

'''
init and argument setting.
'''
init(autoreset=True)

parser = argparse.ArgumentParser(usage="-h for full usage")
parser.add_argument('-url', dest="request_url", help='give a url to crawl, e.g,https://www.facebook.com/search/21435141328/likers', required=True)
parser.add_argument('-file', dest="file", help='output result to file', required=True)
args = parser.parse_args()

def extract_fans_profiles(request_url):	
	user_id_list = list()
	
	driver.get(request_url)
	current_page = 0
	while current_page < 2:
		time.sleep(2)
		current_page += 1
		try:  
			elem = driver.find_element_by_xpath(".//div[@class='_4-u2 _58b7 _3ymv']")
		except:
			print "Invalid graph search query! (hint: first try it on facebook)"
		
		try:
			#print  "current_page=%s" %current_page
			driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

			elem = driver.find_element_by_xpath("//div[@class='phm _64f']")
			if "End of results" in elem.text:
				break
		except:
			pass


	html_source = driver.page_source
	html_lxml = lxml.html.parse(StringIO(html_source)) #parse to lxml object
	
	xpath_fans_area = ".//div[@class='_3u1 _gli _ajr']"	
	fans_area_list = html_lxml.xpath(xpath_fans_area)
	
	xpath_data_bt = "./@data-bt"
	xpath_avatar = ".//a[@class='_8o _8s lfloat _ohe']/img/@src"
	xpath_name = ".//div[@class='_gll']/a/text()"
	
	

	#print "fans_area_list len=%s" %len(fans_area_list)
	
	for fan_area in fans_area_list:	
		try:  
			data_bt = fan_area.xpath(xpath_data_bt)[0]
			##print "data_bt=%s" %data_bt
			user_id = re.search('\{\"id\":(\d+),', data_bt).group(1);
			#print "user_id=%s" %user_id
			
			avatar =  fan_area.xpath(xpath_avatar)[0]
			#print "avatar=%s" %avatar
			
			name = fan_area.xpath(xpath_name)[0]
			#print "name=%s" %name
			
			user_id_list.append(user_id)
		except Exception, e:
			print e
			text = lxml.html.tostring(fan_area)		
			print "text=%s" %text	

		#print "--------------------------------"

	print "Enumerating %s profiles...\n" % len(user_id_list)
	return user_id_list
	

def open_file(file_name):
	results = list()
	with open(file_name, 'r') as in_file:
		for line in in_file.readlines():
			results.append(line.strip())
	return results


def save_file(file_name, results):
	with open(file_name, 'w') as out_file:
			for result in results:
				out_file.write(result.encode('utf8') + "\n")

	print "Saving results to: %s\n\n" % file_name


'''
main logic flow.
'''
display = Display(visible=0, size=(1600, 900))
display.start()	

#webdriver setting.
driver = webdriver.Firefox()
#cookies setting.
cookies = dict()  
#cookies = facebook_login(username, password)
driver.get("http://www.facebook.com")
cookies = pickle.load(open("/home/sa/facebook/python/interest/cookies.pkl", "rb"))
for cookie in cookies:
	driver.add_cookie(cookie)



try:
	#read arguments.
	request_url = args.request_url
	#print "request_url=%s" %request_url
	file_name = args.file
	#print "file=%s" %file_name
	
	#business logic.
	category_to_count = {'test_category': 0}
	page_to_count = {'test_page': 0}
	print "start extract fans profiles"
	user_id_list = extract_fans_profiles(request_url)
	save_file(file_name, user_id_list)
except Exception, e:
	traceback.print_exc(file=sys.stdout)
	print e
finally:
#ending handling.
	print "finally"
	driver.quit()
	display.stop()
	exit()



