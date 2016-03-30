# _*_ coding: utf8 _*_

import time
import urllib
import re
import sqlite3
from bs4 import *

# http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockHolder/stockid/300463/displaytype/30.phtml
# http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockHolder/stockid/300463.phtml

def holder_re_capture(entry, regex = '.*'):
    field_captured = re.findall(regex, entry)
    if len(field_captured) <= 0:
        field_captured = ['-1']
    return field_captured

def gen_url_full(stockid):		#generate the full url
    stockid = str(stockid)
    url_full = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockHolder/stockid/' + stockid + '.phtml'
    return url_full

def get_holders_table(stockid, trying_times = 0):	#get the infomation of stock holder
    try:
        url_full = gen_url_full(stockid)
        holders_html = urllib.urlopen(url_full).read()
        holders_bs = BeautifulSoup(holders_html,'html.parser', from_encoding='gb18030')
        holders_tr_all = holders_bs('tr')
        return holders_tr_all
    except:
        trying_times += 1
        print "Network failure. Retried %d time(s)." % trying_times
        time.sleep(2**trying_times)
        return get_holders_table(stockid)
    

def get_holder_detail(stockid):
    stockid = str(stockid)
    holders_tr_all = get_holders_table(stockid)                           #run the main function
        
    conn = sqlite3.connect('stock.sqlite3')
    cur = conn.cursor()
    try:
        recover_pos_tr = cur.execute('SELECT recoverposition FROM Recover WHERE recovername = ?', ('trrecover', )).fetchone()[0]
        pos_holders = recover_pos_tr
    except:
        recover_pos_tr = 0
        pos_holders = None
    cur.close()
    conn.close()
    
    parsed_tr_counts = recover_pos_tr    
    abort_recover = None
    
    for holder_tr in holders_tr_all[parsed_tr_counts:]:            #looping through holders_tr_all
        if '('+stockid+')  主要股东' in str(holder_tr):          #find the position of the stock holder infomation block
            pos_holders = parsed_tr_counts
            
        if parsed_tr_counts > pos_holders and pos_holders != None:       #list iteam after head of table                
            holder_td_all = holder_tr.find_all('td')           #find all of the 'td' tags
            holder_td_all_len = holder_td_all.__len__()           #get the length of every 'td' tag
            holder_td_all_str = str(holder_td_all).decode('unicode-escape').encode('utf8')
            
            if holder_td_all_len >= 2:
                if holder_td_all_len == 2:                 #basic information of the table
                    is_date_upto = re.search('截至日期',holder_td_all_str)      #find 截至日期
                    if is_date_upto:
                        date_upto = re.findall('>([\d-]*(?:-)[\d-]*)<', holder_td_all_str)     #get 截至日期
                        if len(date_upto) <= 0: date_upto = ['-1']
                    is_date_report = re.search('公告日期',holder_td_all_str)    #find 公告日期
                    if is_date_report:
                        date_report = re.findall('>([\d-]*(?:-)[\d-]*)<', holder_td_all_str)   #get 公告日期
                        if len(date_report) <= 0: date_report = ['-1']
                    is_num_holders = re.search('股东总数',holder_td_all_str)    #find 股东总数
                    if is_num_holders:
                        num_holders = re.findall('>[^0-9<>]*?(\d+)[^0-9<>]*?<', holder_td_all_str)   #get 股东总数
                        if len(num_holders) <= 0: num_holders = ['-1']
                    is_avg_shares = re.search('平均持股数',holder_td_all_str)    #find 平均持股数
                    if is_avg_shares:
                        avg_shares = re.findall('>[^0-9<>]*?(\d+)[^0-9<>]*?<', holder_td_all_str)   #get 平均持股数
                        if len(avg_shares) <= 0: avg_shares = ['-1']
                        
                if holder_td_all_len == 5:                 #details of shareholder 
                        holder_entry = list()
                        for holder_td in holder_td_all:
                            holder_td_field = re.findall('(?!>(?:\\xc2\\xa0)*<|>[^<>]*\\xe2\\x86\\x91[^<>]*<|>[^<>]*\\xe2\\x86\\x93[^<>]*<)>(?:\\xc2\\xa0)*([^<>]+?)(?:\\xc2\\xa0)*<', 
                                                         str(holder_td))           #get the list of detail of shareholder
                            if len(holder_td_field) < 1:
                                holder_td_field = ['-1']
                            elif len(holder_td_field) > 1:
                                holder_td_field = ['Regex error']
                            holder_entry.extend(holder_td_field)
                        
                        holder_id = re.findall('stockholderid=(\d+)"', holder_td_all_str)
                        if len(holder_id) <= 0: holder_id= holder_entry[1:2]
                        
                        if '编号' in holder_entry: 
                            parsed_tr_counts += 1
                            continue
                        
                        holder_entry.extend(holder_id)
                        holder_entry.append(stockid)
                        try:
                            holder_entry.extend(date_upto)                   #append the up-to date in the list of detail of shareholder
                            holder_entry.extend(date_report)                 #append the report date in the list of detail of shareholder
                            holder_entry.extend(num_holders)                 #append the number of holders in the list of detail of shareholder
                            holder_entry.extend(avg_shares)                  #append the averge shares of every holder in the list of detail of shareholder
                        except:
                            abort_recover = True
                                                
                        for holder_entry_index in range(len(holder_entry)):
                            holder_entry[holder_entry_index] = holder_entry[holder_entry_index].decode('utf-8')
                        
                        conn = sqlite3.connect('stock.sqlite3')
                        #conn.text_factory = str
                        cur = conn.cursor()
                        cur.execute('CREATE TABLE IF NOT EXISTS Majorholderinfo (currentrank TEXT ,shareholder TEXT, quantity TEXT, percentage TEXT, equitytype TEXT, holderid TEXT, stockid TEXT, uptodate TEXT, reportdate TEXT, holdersnumber TEXT, averageshares TEXT)')
                        if abort_recover:
                            cur.execute('INSERT INTO Majorholderinfo (currentrank, shareholder, quantity, percentage, equitytype, holderid, stockid, uptodate, reportdate, holdersnumber, averageshares) SELECT ?, ?, ?, ?, ?, ?, ?, uptodate, reportdate, holdersnumber, averageshares FROM Majorholderinfo WHERE rowid = (SELECT MAX(rowid) FROM Majorholderinfo)', 
                                        (holder_entry[0], holder_entry[1], holder_entry[2], holder_entry[3], holder_entry[4], holder_entry[5], holder_entry[6])
                                        )
                        else:
                            cur.execute('INSERT INTO Majorholderinfo (currentrank, shareholder, quantity, percentage, equitytype, holderid, stockid, uptodate, reportdate, holdersnumber, averageshares) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', 
                                        (holder_entry[0], holder_entry[1], holder_entry[2], holder_entry[3], holder_entry[4], holder_entry[5], holder_entry[6], holder_entry[7], holder_entry[8], holder_entry[9], holder_entry[10])
                                        )
                        cur.execute('INSERT OR IGNORE INTO Recover (rowid, recovername, recoverposition) VALUES (?, ?, ?)', 
                                    (1, 'trrecover', parsed_tr_counts)
                                    )
                        cur.execute('UPDATE Recover SET recoverposition = ? WHERE recovername = ?', 
                                    (parsed_tr_counts, 'trrecover')
                                    )
                        conn.commit()
                        cur.close()
                        conn.close()
                        
                        print str(holder_entry).decode('unicode-escape')
                        #print holder_entry[0], holder_entry[1], holder_entry[2], holder_entry[3], holder_entry[4], holder_entry[5], holder_entry[6], holder_entry[7], holder_entry[8], holder_entry[9], holder_entry[10]
                        
        parsed_tr_counts += 1
    
def get_full_holders_info():
    conn = sqlite3.connect('stock.sqlite3')
    cur = conn.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS Recover (rowid INTEGER, recovername TEXT PRIMARY KEY, recoverposition INTEGER) WITHOUT ROWID')
    try:
        cur.execute('SELECT recoverposition FROM Recover WHERE recovername = ?', 
                    ('stockidrecover', )
                    )
        recover_pos_stockid = cur.fetchone()[0]
    except:
        recover_pos_stockid = 0
    cur.close()
    conn.close()
    
    parsed_stockid_counts = recover_pos_stockid
    fhand_stock = open('stock.txt','r')
    flines_stock = fhand_stock.readlines()
    for stockid in flines_stock[recover_pos_stockid:]:
        stockid = stockid.rstrip()
        get_holder_detail(stockid)
        parsed_stockid_counts += 1
        
        conn = sqlite3.connect('stock.sqlite3')
        cur = conn.cursor()
        cur.execute('UPDATE Recover SET recoverposition = ? WHERE recovername = ?', 
                    (0, 'trrecover')
                    )
        cur.execute('INSERT OR IGNORE INTO Recover (rowid, recovername, recoverposition) VALUES (?, ?, ?)', 
                    (0 ,'stockidrecover', parsed_stockid_counts)
                    )
        cur.execute('UPDATE Recover SET recoverposition = ? WHERE recovername = ?', 
                    (parsed_stockid_counts, 'stockidrecover')
                    )
        conn.commit()
        cur.close()
        conn.close()
    fhand_stock.close()
    
get_full_holders_info()
