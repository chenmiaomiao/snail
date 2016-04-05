# _*_ coding: utf8 _*_

import time
import datetime
import urllib
import json

#http://q.stock.sohu.com/hisHq?code=cn_600028&start=20150918&end=20160115&stat=1&order=D&period=d&callback=historySearchHandler&rt=jsonp&r=0.5620633495524285&0.07780711725972944

# convert date to string, vice versa
def date_interconvert(date_data):
    if type(date_data) != datetime.datetime:
        if u'-' in date_data:
            return datetime.datetime.strptime(date_data, '%Y-%m-%d')
        else:
            return datetime.datetime.strptime(date_data, '%Y%m%d')
    if type(date_data)== datetime.datetime:
        return date_data.strftime('%Y%m%d')

class stock_price:
    def __init__(self):
        self.price_close = []
        self.price_c_all = []
        self.price_all = []
    
    # generate the full url
    def gen_full_url(self, stockid, startdate, enddate):
        stockid = str(stockid)
        startdate = date_interconvert(startdate)
        enddate = date_interconvert(enddate)
        full_url = 'http://q.stock.sohu.com'+ '/hisHq?code=cn_' + stockid + '&start=' + startdate + '&end=' + enddate        
        return full_url
    
    # get the prices of a period
    def get_price_all(self, stockid, startdate, enddate, trying_times = 0):
        try:
            full_url = self.gen_full_url(stockid, startdate, enddate)
            html = urllib.urlopen(full_url).read()
            market_info = json.loads(html)
            try:
                self.price_all = market_info[0]['hq']
            except:
                self.price_all = []
            return self.price_all
        except:
            trying_times += 1
            print "Network failure. Retried %d time(s)." % trying_times
            time.sleep(trying_times)
            return self.get_price_all(stockid, startdate, enddate, trying_times)
    
    # get the close price of a period
    def get_price_close_all(self, stockid, startdate, enddate):
        price_all = self.get_price_all(stockid, startdate, enddate)
        
        self.price_c_all = []
        for price_day in price_all:
            date_d = date_interconvert(price_day[0])
            price_c = price_day[2]
            self.price_c_all.append((date_d, price_c))
            
        return self.price_c_all
    
    # get the close price of specific day
    def get_price_close(self, stockid, date, fuzzy_mode = True, fuzzy_forward = True):
        if fuzzy_forward:
            unix = time.time()
            fuzzy_direction = 1
            time_stamp = datetime.datetime.fromtimestamp(unix)
        else:
            fuzzy_direction = -1
            time_stamp = date_interconvert('1990-01-01')
        
        self.price_close = None
        
        price_day = self.get_price_all(stockid, date, date)
        if len(price_day) > 0:
            self.price_close = price_day[0][2]
            date_end = date + datetime.timedelta(90)
        elif fuzzy_mode:
            print 'Stockid: %s, date Start: %s. Trying...' % (stockid, date)
            if fuzzy_forward:
                price_c_all = self.get_price_close_all(stockid, date, time_stamp)
            else:
                price_c_all = self.get_price_close_all(stockid, time_stamp, date)
            if len(price_c_all) > 0: 
                for price_c in price_c_all[::-fuzzy_direction]:
                    if price_c[0] > date:
                        self.price_close = price_c[1]
                        date = price_c[0]
                        date_end = date + datetime.timedelta(90*fuzzy_direction)
                        print 'Stockid: %s, date Start: %s. Gotten.' % (stockid, date)
                        break
        
        if self.price_close == None: 
            return self.price_close
        else:
            return self.price_close, date, date_end, fuzzy_direction
    
if __name__ == '__main__':
    query_stock_price = stock_price()
    date_0 = date_interconvert('1999-12-20')
    price_d = query_stock_price.get_price_close(600028, date_0)
    print price_d
    # date_1 = date_interconvert('20160101')
    # price_c_all = query_stock_price.get_price_close_all(600028, date_0, date_1)
    # print price_c_all
