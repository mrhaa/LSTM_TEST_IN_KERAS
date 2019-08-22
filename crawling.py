import urllib
import urllib.request
import requests
from urllib.error import HTTPError

from bs4 import BeautifulSoup
from selenium import webdriver
import datetime
import time
from itertools import count
import arrow
import pandas as pd


class Good():
    def __init__(self):
        self.value = "+"
        self.name = "good"

    def __repr__(self):
        return "<Good(value='%s')>" % (self.value)


class Bad():
    def __init__(self):
        self.value = "-"
        self.name = "bad"

    def __repr__(self):
        return "<Bad(value='%s')>" % (self.value)


class Unknow():
    def __init__(self):
        self.value = "?"
        self.name = "unknow"

    def __repr__(self):
        return "<Unknow(value='%s')>" % (self.value)


class InvestingEconomicCalendar():
    def __init__(self, uri='https://www.investing.com/economic-calendar/', country_list = None):
        self.uri = uri
        self.req = urllib.request.Request(uri)
        self.req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36')
        self.country_list = country_list
        self.result = []

    def getEvents(self):
        try:
            response = urllib.request.urlopen(self.req)
            html = response.read()
            soup = BeautifulSoup(html, "html.parser")

            # Find event item fields
            table = soup.find('table', {"id": "economicCalendarData"})
            tbody = table.find('tbody')
            rows = tbody.findAll('tr', {"class": "js-event-item"})

            for row in rows:
                events = {}
                _datetime = row.attrs['data-event-datetime']
                events['timestamp'] = arrow.get(_datetime, "YYYY/MM/DD HH:mm:ss").timestamp
                events['date'] = _datetime[0:10]

                cols = row.find('td', {"class": "flagCur"})
                flag = cols.find('span')
                events['country'] = flag.get('title')

                # 예외 국가 필터링
                if self.country_list is not None and events['country'] not in self.country_list:
                    continue

                impact = row.find('td', {"class": "sentiment"})
                bull = impact.findAll('i', {"class": "grayFullBullishIcon"})
                events['impact'] = len(bull)

                event = row.find('td', {"class": "event"})
                a = event.find('a')
                events['url'] = "{}{}".format(self.uri, a['href'][a['href'].find('/',2)+1:])
                events['name'] = a.text.strip()

                # Determite type of event
                events['type'] = None
                if event.find('span', {"class": "smallGrayReport"}):
                    events['type'] = "report"
                elif event.find('span', {"class": "audioIconNew"}):
                    events['type'] = "speech"
                elif event.find('span', {"class": "smallGrayP"}):
                    events['type'] = "release"
                elif event.find('span', {"class": "sandClock"}):
                    events['type'] = "retrieving data"

                bold = row.find('td', {"class": "bold"})
                events['bold'] = bold.text.strip() if bold.text != '' else ''

                fore = row.find('td', {"class": "fore"})
                events['fore'] = fore.text.strip() if fore.text != '' else ''

                prev = row.find('td', {"class": "prev"})
                events['prev'] = prev.text.strip() if prev.text != '' else ''

                if "blackFont" in bold['class']:
                    events['signal'] = Unknow()
                elif "redFont" in bold['class']:
                    events['signal'] = Bad()
                elif "greenFont" in bold['class']:
                    events['signal'] = Good()
                else:
                    events['signal'] = Unknow()

                print(events)
                self.result.append(events)

        except HTTPError as error:
            print("Oops... Get error HTTP {}".format(error.code))

        return self.result


class InvestingEconomicEventCalendar():
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        if 0:
            self.options.add_argument('headless')
            self.options.add_argument('window-size=1920x1080')
            self.options.add_argument("disable-gpu")
            # 혹은 options.add_argument("--disable-gpu")

        #self.wd = webdriver.Chrome('chromedriver', chrome_options=self.options)
        self.wd = webdriver.Chrome('/Users/sangjinryu/Downloads/chromedriver', chrome_options=self.options)
        self.wd.get('https://www.investing.com')
        time.sleep(60)


    def GetEventSchedule(self, url, cd):

        self.wd.get(url)

        RESULT_DIRECTORY = '__results__/crawling'
        results = []
        for page in count(1):
            try:
                script = 'void(0)'  # 사용하는 페이지를 이동시키는 js 코드
                #self.wd.execute_script(script)  # js 실행
                result = self.wd.find_element_by_xpath('//*[@id="showMoreHistory%s"]/a' % cd)
                result.click()

                time.sleep(0.2)              # 크롤링 로직을 수행하기 위해 5초정도 쉬어준다.

            except:
                #print('error: %s' % str(page))

                html = self.wd.page_source
                bs = BeautifulSoup(html, 'html.parser')
                nm = bs.find('body').find('section').find('h1').text
                tbody = bs.find('tbody')
                rows = tbody.findAll('tr')

                for row in rows:
                    tmp_rlt = {}

                    times = row.findAll('td', {'class': 'left'})
                    tmp_rlt['date'] = times[0].text.strip()
                    tmp_rlt['time'] = times[1].text.strip()
                    tmp_rlt['pre_release'] = True if times[1].find('span') != None and times[1].find('span')['title'] == "Preliminary Release" else False

                    values = row.findAll('td', {'class': 'noWrap'})
                    tmp_rlt['bold'] = values[0].text.strip()
                    tmp_rlt['fore'] = values[1].text.strip()
                    tmp_rlt['prev'] = values[2].text.strip()
                    #print(tmp_rlt)

                    results.append(tmp_rlt)

                return nm, results


class IndiceHistoricalData():

	def __init__(self, API_url):
		self.API_url = API_url

		# set https header parameters
		headers = {
			'User-Agent': 'Mozilla/5.0',  # required
			'referer': "https://www.investing.com",
			'host': 'www.investing.com',
			'X-Requested-With': 'XMLHttpRequest'
		}
		self.headers = headers

	#set indice data (indices.py)
	def setFormData(self, data):
		self.data = data

	#prices frequency, possible values: Monthly, Weekly, Daily
	def updateFrequency(self, frequency):
		self.data['frequency'] = frequency

	#desired time period from/to
	def updateStartingEndingDate(self, startingDate, endingDate):
		self.data['st_date'] = startingDate
		self.data['end_date'] = endingDate

	#possible values: 'DESC', 'ASC'
	def setSortOreder(self, sorting_order):
		self.data['sort_ord'] = sorting_order

	#making the post request
	def downloadData(self):
		self.response = requests.post(self.API_url, data=self.data, headers=self.headers).content
		#parse tables with pandas - [0] probably there is only one html table in response
		self.observations = pd.read_html(self.response)[0]
		return self.observations

	#print retrieved data
	def printData(self):
		print(self.observations)

	#print retrieved data
	def saveDataCSV(self):
		self.observations.to_csv(self.data['name']+'.csv', sep='\t', encoding='utf-8')


#https://www.investing.com/commodities/gold-historical-data
GC = {
	'name' : 'GC',
	'curr_id': 8830,
	'smlID': 300004,
	'header' : 'Gold Futures',
	'sort_col' : 'date',
	'action' : 'historical_data'
}

#https://www.investing.com/commodities/copper-historical-data
HG = {
	'name' : 'HG',
	'curr_id': 8831,
	'smlID': 300012,
	'header' : 'Copper Futures',
	'sort_col' : 'date',
	'action' : 'historical_data'
}

#https://www.investing.com/commodities/silver-historical-data
SI = {
	'name' : 'SI',
	'curr_id': 8836,
	'smlID': 300044,
	'header' : 'Silver Futures',
	'sort_col' : 'date',
	'action' : 'historical_data'
}

#https://www.investing.com/commodities/natural-gas-historical-data
NG = {
	'name' : 'NG',
	'curr_id': 8862,
	'smlID': 300092,
	'header' : 'Natural Gas Futures',
	'sort_col' : 'date',
	'action' : 'historical_data'
}

#https://www.investing.com/commodities/crude-oil-historical-data
CL = {
	'name' : 'CL',
	'curr_id': 8849,
	'smlID': 300060,
	'header' : 'Crude Oil WTI Futures',
	'sort_col' : 'date',
	'action' : 'historical_data'
}

#https://www.investing.com/indices/us-30-historical-data
DJI = {
	'name' : 'DJI',
	'curr_id': 169,
	'smlID': 2030170,
	'header' : 'Dow Jones Industrial Average',
	'sort_col' : 'date',
	'action' : 'historical_data'
}

#https://www.investing.com/indices/us-spx-500-historical-data
SPX = {
	'name' : 'SPX',
	'curr_id': 166,
	'smlID': 2030167,
	'header' : 'S&P 500',
	'sort_col' : 'date',
	'action' : 'historical_data'
}

#https://www.investing.com/indices/nq-100-historical-data
NDX = {
	'name' : 'NDX',
	'curr_id': 20,
	'smlID': 2030165,
	'header' : 'Nasdaq 100',
	'sort_col' : 'date',
	'action' : 'historical_data'
}

#https://www.investing.com/indices/eu-stoxx50-historical-data
STOXX50E = {
	'name' : 'STOXX50E',
	'curr_id': 175,
	'smlID': 2030175,
	'header' : 'Euro Stoxx 50',
	'sort_col' : 'date',
	'action' : 'historical_data'
}