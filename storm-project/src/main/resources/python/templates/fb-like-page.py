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
parser.add_argument('-url', dest="request_url", help='give a url to crawl, e.g,https://www.facebook.com/search/171522852874952/pages-liked', required=True)
parser.add_argument('-file', dest="file", help='output result to file', required=True)
args = parser.parse_args()

def extract_fans_like_pages(fan_like_page_url):
	#print "fan_like_page_url: %s" % fan_like_page_url
	
	page_list = list();
	
	driver.get(fan_like_page_url)
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

	xpath_page_area = ".//div[@class='_3u1 _gli _ajr']"	
	page_area_list = html_lxml.xpath(xpath_page_area)
	
	xpath_data_bt = "./@data-bt"
	xpath_page_url = ".//a[@class='_8o _8s lfloat _ohe']/@href"
	xpath_page_category = ".//div[@class='_pac']/text()"
	xpath_page_category_special = ".//div[@class='_pac']/a/text()"

	#print "page_area_list len=%s" %len(page_area_list)

	for page_area in page_area_list:	
		try:  
			data_bt = page_area.xpath(xpath_data_bt)[0]
			#print "data_bt=%s" %data_bt
			page_id = re.search('\{\"id\":(\d+),', data_bt).group(1);
			#print "page_id=%s" %page_id
			
			page_url = page_area.xpath(xpath_page_url)[0]
			#print "page_url=%s" %page_url
			
			page_name = re.search('facebook.com\/pages\/(.*)\/\d+\?', page_url);						
			if page_name == None:
				page_name = re.search('facebook.com\/(.*)\?', page_url);
			
			page_name = page_name.group(1).strip();
			#print "page_name=%s" %page_name
			
			page_category_list = page_area.xpath(xpath_page_category)
			if len(page_category_list) <= 0:
				#print "try xpath_page_category_special"
				page_category_list = page_area.xpath(xpath_page_category_special)
							
			page_category = page_category_list[0].strip()					
			#print "page_category=%s" %page_category				
			
			page_list.append(page_id + "," + page_name + "," + page_category + "," + page_url)
		except Exception, e:
			traceback.print_exc(file=sys.stdout)
			print e
			text = lxml.html.tostring(page_area)		
			print "text=%s" %text	
	
		#print "--------------------------------"

	print "Enumerating %s like pages...\n" % len(page_list)
	return page_list


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
	print "start extract fans like pages"
	page_list = extract_fans_like_pages(request_url);
	save_file(file_name, page_list)
except Exception, e:
	traceback.print_exc(file=sys.stdout)
	print e
finally:
#ending handling.
	print "finally"
	driver.quit()
	display.stop()
	exit()



