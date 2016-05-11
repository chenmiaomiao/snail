# _*_ coding: utf8 _*_

import time
import urllib
import urllib2
import httplib
import json
from datetime import datetime, timedelta
from _socket import timeout
from urllib2 import URLError

# http://q.stock.sohu.com/hisHq?code=cn_600028&start=20150918&end=20160115&stat=1&order=D&period=d&callback=historySearchHandler&rt=jsonp&r=0.5620633495524285&0.07780711725972944

def gen_headers(host, referer):
    headers = {}
    
    headers['Host'] = host
    headers['Referer'] = referer
    headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebkit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.89 Safari/537.36'
    headers['Accept-Language'] = 'zh-CN,zh;q=0.8,ja;q=0.6, en-us;0.4'
    # headers['Accept-Encoding'] = 'gzip, deflate, sdch'
    headers['Keep-Alive'] = '300'
    headers['Connection'] = 'keep-alive'
    headers['Cache-Control'] = 'max-age=0'
    headers['Accept'] = 'text/html, */*; q=0.01'
    headers['X-Requested-With'] = 'XMLHttpRequest'
    headers['DNT'] = '1'

    return headers
    
# convert date to string, vice versa
def date_interconvert(date_data):
    if type(date_data) != datetime:
        return datetime.strptime(date_data, '%Y-%m-%d')
    if type(date_data)== datetime:
        return date_data.strftime('%Y%m%d')

# generate the full url
def gen_full_url(stockid, startdate, enddate):
    stockid = str(stockid)
    startdate = date_interconvert(startdate)
    enddate = date_interconvert(enddate)
    full_url = 'http://q.stock.sohu.com'+ '/hisHq?code=cn_' + stockid + '&start=' + startdate + '&end=' + enddate
    full_url_without_root = '/hisHq?code=cn_' + stockid + '&start=' + startdate + '&end=' + enddate
    # full_url = 'http://www.pydev.org/manual_adv_debugger.html'     
    return full_url, full_url_without_root

# get the prices of a period
def get_price_all(stockid, startdate, enddate, tried_times = 0, timeout = 3):
    stockid_str = str(stockid)
    
    host = 'q.stock.sohu.com'
    referer = 'http://q.stock.sohu.com/cn/'+stockid_str+'/index.shtml'
    header = gen_headers(host, referer)
    
    full_url, full_url_withou_root = gen_full_url(stockid, startdate, enddate)
    
    try:
        # html = urllib.urlopen(full_url).read()
        #=======================================================================
        # conn = httplib.HTTPConnection("q.stock.sohu.com", timeout = timeout)
        # conn.request('GET', full_url_withou_root)
        # rsp = conn.getresponse()
        # html = rsp.read()
        #=======================================================================
        data = None
        req = urllib2.Request(full_url, data, header)
        respnse = urllib2.urlopen(req, timeout = timeout)
        html = respnse.read()
        market_info = json.loads(html)
        
        try:
            price_all = market_info[0]['hq']
        except:
            price_all = []
            
        return price_all
    except URLError, e:
        if 'timed out' in str(e) and tried_times <= 60:
            tried_times += 1
            print 'Time out. Retried %d time(s).' % tried_times
            time.sleep(tried_times*tried_times)
            return get_price_all(stockid, startdate, enddate, tried_times)
        elif 'failed' in str(e):
            tried_times += 1
            print 'Network failure. Retried %d time(s).' % tried_times
            time.sleep(tried_times*tried_times)
            return get_price_all(stockid, startdate, enddate, tried_times)
        else:
            print 'Sorry, I have tried my best! Already tried %d time(s).' % tried_times
            return []
    except Exception, e:
        print 'Sorry, I cannot get it! The error is %s' % e
        return []

# get the close price of a period
def get_price_close_all(stockid, startdate, enddate):
    price_all = get_price_all(stockid, startdate, enddate)
    
    price_c_all = []
    for price_day in price_all:
        date_d = date_interconvert(price_day[0])
        price_c = price_day[2]
        price_c_all.append((date_d, price_c))
        
    return price_c_all

# get the close price of specific day
def get_price_close(stockid, date, fuzzy_mode = True, fuzzy_forward = True):
    if fuzzy_forward:
        unix = time.time()
        fuzzy_direction = 1
        time_stamp = datetime.fromtimestamp(unix)
    else:
        fuzzy_direction = -1
        time_stamp = date_interconvert('1990-01-01')
    
    price_close = None
    
    price_day = get_price_all(stockid, date, date)
    if len(price_day) > 0:
        price_close = price_day[0][2]
        date_end = date + timedelta(90)
    elif fuzzy_mode:
        print 'Stockid: %s, date Start: %s. Trying...' % (stockid, date)
        if fuzzy_forward:
            price_c_all = get_price_close_all(stockid, date, time_stamp)
        else:
            price_c_all = get_price_close_all(stockid, time_stamp, date)
        if len(price_c_all) > 0: 
            for price_c in price_c_all[::-fuzzy_direction]:
                if price_c[0] > date:
                    price_close = price_c[1]
                    date = price_c[0]
                    date_end = date + timedelta(90*fuzzy_direction)
                    print 'Stockid: %s, date Start: %s. Gotten.' % (stockid, date)
                    break
    
    #===========================================================================
    # price_close = 30
    # date = 0
    # date_end = 0 
    #===========================================================================
    
    if price_close == None: 
        return price_close
    else:
        return price_close, date, date_end, fuzzy_direction
    
if __name__ == '__main__':
    date_0 = date_interconvert('1999-12-20')
    price_d = get_price_close(600028, date_0)
    print price_d
    # date_1 = date_interconvert('20160101')
    # price_c_all = query_stock_price.get_price_close_all(600028, date_0, date_1)
    # print price_c_all
