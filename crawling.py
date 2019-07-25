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
    def __init__(self, uri='https://www.investing.com/economic-calendar/'):
        self.uri = uri
        self.req = urllib.request.Request(uri)
        self.req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36')
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


def InvestingEconomicEventCalendar(url, cd):


    # \U는 유니코드로 인식되기 때문에 \\U와 같이 escape 처리했다.
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_argument('window-size=1920x1080')
    options.add_argument("disable-gpu")
    # 혹은 options.add_argument("--disable-gpu")

    wd = webdriver.Chrome('chromedriver', chrome_options=options)
    wd.get(url)


    try:
        RESULT_DIRECTORY = '__results__/crawling'
        results = []
        for page in count(1):
            #script = 'store.getList(%d)' % page  # 굽네치킨에서 사용하는 페이지를 이동시키는 js 코드
            script = 'void(0)'  # 굽네치킨에서 사용하는 페이지를 이동시키는 js 코드
            #wd.execute_script(script)  # js 실행
            result = wd.find_element_by_xpath('//*[@id="showMoreHistory%s"]/a' % cd)
            result.click()

            time.sleep(1)              # 크롤링 로직을 수행하기 위해 5초정도 쉬어준다.



    except:
        print('error: %s' % str(page))

        html = wd.page_source
        bs = BeautifulSoup(html, 'html.parser')
        tbody = bs.find('tbody')
        rows = tbody.findAll('tr')

        for row in rows:
            tmp_rlt = {}

            times = row.findAll('td', {'class': 'left'})
            tmp_rlt['date'] = times[0].text.strip()
            tmp_rlt['time'] = times[1].text.strip()

            values = row.findAll('td', {'class': 'noWrap'})
            tmp_rlt['bold'] = values[0].text.strip()
            tmp_rlt['fore'] = values[1].text.strip()
            tmp_rlt['prev'] = values[2].text.strip()
            #print(tmp_rlt)

            results.append(tmp_rlt)

    return results