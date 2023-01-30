import scrapy
import json
from operator import itemgetter
import re
import csv
import os, sys
from datetime import datetime
from scrapy.http import Request
from scrapy.shell import inspect_response
from scrapy.exceptions import CloseSpider

class BlogSpider(scrapy.Spider):
    name="parker"

    counter = 0
    Recursive_List = []
    handle_httpstatus_list = [302]
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'CONCURRENT_REQUESTS': '30',
        'CLOSESPIDER_ERRORCOUNT': '1',
        'LOG_FILE_APPEND' : 'False',
        'REDIRECT_ENABLED': 'False',
        'RETRY_ENABLED':'False',
        #'LOG_FILE' : 'C:\\Users\\Jackie\\.virtualenvs\\BeautifulSoup_Project\\Proj4\\log\\abc.log',
        #'FEED_URI': 'file:///C:\\Users\\Jackie\\.virtualenvs\\BeautifulSoup_Project\\Proj4\\output\\abc.csv',
        #'AUTOTHROTTLE_ENABLED': 'true',
        #'DOWNLOAD_DELAY': '5',
        #'CONCURRENT_REQUESTS_PER_IP' : '30',
        #'CONCURRENT_REQUESTS_PER_DOMAIN' : '',
        #'MAX_CONCURRENT_REQUESTS_PER_DOMAIN' : ''
    }

    def __init__(self, input_query_filepath = '', **kwargs):
        super().__init__(**kwargs)

        print("Filepath provided - %s" % input_query_filepath)

        if not os.path.exists(input_query_filepath):
            print("Filepath provided - %s" % input_query_filepath)

        ### Get Target DNS to search
        self.start_urls = []
        with open(input_query_filepath, mode='r',encoding='UTF-8') as file:
            for line in file:
                self.start_urls.append(line.rstrip())


    def parse(self, response):
        self.counter += 1
        self.Recursive_List = []
        Found_Dns = [i for i in response.css('a::attr(href)').extract() if i.startswith("/url") and i.find(".google.") == -1]
        
        if response.status == 302:
            print("Terminating...")
            raise CloseSpider('Terminated')

        resul = ""
        result_Dns = []
        found_flag = False
        temp =""
        if len(Found_Dns) > 0:
            response_url = response.url.replace('&tbm=nws','')
            response_url = response_url.split('+OR+')
            Queried_Dns = [i.split('inurl%3A')[1] for i in response_url]

            for j in Found_Dns:
                temp2 = re.search('https://(.+?)/', j)
                if temp2:
                    temp2 = temp2.group(1)
                
                for k in Queried_Dns:
                    if temp2 == k or temp2 == ("www." + k):
                        found_flag=True
                        Queried_Dns.remove(k)
                        print("%s Found" % self.counter)
                        resul = "Found"
                        result_Dns.append(k)
                        break

            if not found_flag:
                resul = "Searched and Not Found"
                print("%s Searched and Not Found" % self.counter)
            else:
                a = response.url
                for i in result_Dns:
                    temp = "inurl%3A" + str(i)
                    a = a.replace(temp,'')
                    a = a.replace('q=+OR+','q=')
                    a = a.replace('+OR++OR+','+OR+')
                    a = a.replace('+OR+&tbm','&tbm')
                
                self.Recursive_List.append(a)
        else:
            resul = "Not Found"
            print("%s None found" % self.counter)
        
        '''
        #inspect_response(response, self)
        '''

        if len(self.Recursive_List) > 0:
            print("inside IF..")
            for i in self.Recursive_List:
                yield Request(i, callback=self.parse)
        
        yield {'Query': response.url, 
                'Result': resul,
                'Found_Count':len(result_Dns),
                'Found_Dns':",".join(result_Dns)
        }
