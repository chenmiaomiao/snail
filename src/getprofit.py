# _*_ coding: utf8 _*_

import threading
import time
import datetime
import sqlite3
from getprice import date_interconvert, stock_price

conn = sqlite3.connect('stock.sqlite3')
cur = conn.cursor()

# calculate the max date in given dates
def get_max_date(*dates):
    current_max_date = None
    for date in dates:
        if current_max_date == None:
            current_max_date = date
        elif date > current_max_date:
            current_max_date = date
    return current_max_date

# get the profit of holder from internet
def get_holder_profit(stockid, date):
    date_start = date
    query_price = stock_price()
    
    price_start_full = query_price.get_price_close(stockid, date)
    if price_start_full != None:
        price_start = price_start_full[0]
        date_start = price_start_full[1]
        date_end = price_start_full[2]
        price_end_full = query_price.get_price_close(stockid, date_end)
        if price_end_full != None:
            price_end = price_end_full[0]
            date_end = price_end_full[1]
        else:
            return None
    else:
        return None
    
    holder_profit = (float(price_end) - float(price_start))/float(price_start)
    return holder_profit, date_start, date_end, price_start, price_end

# pull out holder portfolio and add a column of profit
def create_table_profit(task_rowid):
    #===========================================================================
    # conn = sqlite3.connect('stock.sqlite3')
    # cur = conn.cursor()
    # try:
    #     cur.execute('SELECT recoverposition FROM Recover WHERE recovername = ?', 
    #                 ('fetchprofitrecover', )
    #                 )
    #     recover_pos_profit = cur.fetchone()[0]
    # except:
    #     recover_pos_profit = 0
    # cur.execute("CREATE TABLE IF NOT EXISTS Holderprofit (rowid INTEGER PRIMARY KEY, holderid TEXT, stockid TEXT, uptodate TEXT, reportdate TEXT, holderprofit REAL, startdate TEXT, enddate TEXT, startprice TEXT, endprice TEXT)")
    # cur.execute('CREATE TABLE IF NOT EXISTS Recover (rowid INTEGER, recovername TEXT PRIMARY KEY, recoverposition INTEGER) WITHOUT ROWID')
    #===========================================================================
    lock = threading.Lock()
    
    lock.acquire()
    dbconn=sqlite3.connect('stock.sqlite3')
    dbcur = dbconn.cursor()
    result_item = dbcur.execute("SELECT rowid, holderid, stockid, uptodate, reportdate FROM Majorholderinfo WHERE rowid = ? AND holderid <> -1 AND uptodate <> -1 AND reportdate <> -1 AND holderid <> 'Regex error' AND uptodate <> 'Regex error' AND reportdate <> 'Regex error' ORDER BY rowid ASC", (task_rowid, )).fetchone()
    
    if result_item is not None:
        row_id = result_item[0]
        #=======================================================================
        # if recover_pos_profit >= row_id:
        #     continue
        #=======================================================================
        holder_id = result_item[1]
        stock_id = result_item[2]
        upto_date =  result_item[3]
        report_date = result_item[4]
        upto_date_timef = date_interconvert(upto_date)
        report_date_timef = date_interconvert(report_date)
        max_date = get_max_date(upto_date_timef, report_date_timef)
        holder_profit = get_holder_profit(stock_id, max_date)
        
        if holder_profit == None:
            dbcur.execute("DELETE FROM Unfinishedtasks WHERE rowid = ?", (task_rowid, ))
        else:
            profit_entry = []
            profit_entry.append(row_id)
            profit_entry.append(holder_id)
            profit_entry.append(stock_id)
            profit_entry.append(upto_date)
            profit_entry.append(report_date)
            profit_entry.extend(holder_profit)
            
            print profit_entry
            
            dbcur.execute("INSERT OR IGNORE INTO Holderprofit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", profit_entry)
            dbcur.execute('INSERT OR IGNORE INTO Recover (rowid, recovername, recoverposition) VALUES (?, ?, ?)', 
                        (2 ,'fetchprofitrecover', row_id)
                        )
            dbcur.execute('UPDATE Recover SET recoverposition = ? WHERE recovername = ?', 
                        (row_id, 'fetchprofitrecover')
                        )
            dbcur.execute("DELETE FROM Unfinishedtasks WHERE rowid = ?", (task_rowid, ))
    else:
        dbcur.execute("DELETE FROM Unfinishedtasks WHERE rowid = ?", (task_rowid, ))
    
    dbconn.commit()
    dbcur.close()
    dbconn.close()
    lock.release()
    
def multi_thread(threads, method):

    cur.execute("CREATE TABLE IF NOT EXISTS Unfinishedtasks (rowid INTEGER PRIMARY KEY, assigned BOOLEAN DEFAULT 0) WITHOUT ROWID")
    cur.execute("CREATE TABLE IF NOT EXISTS Holderprofit (rowid INTEGER PRIMARY KEY, holderid TEXT, stockid TEXT, uptodate TEXT, reportdate TEXT, holderprofit REAL, startdate TEXT, enddate TEXT, startprice TEXT, endprice TEXT)")
    cur.execute("INSERT OR IGNORE INTO Unfinishedtasks (rowid, assigned) SELECT Majorholderinfo.rowid, 0 FROM Majorholderinfo LEFT OUTER JOIN Holderprofit on Holderprofit.rowid = Majorholderinfo.rowid WHERE (((Holderprofit.rowid) IS NULL AND Majorholderinfo.rowid > (SELECT MAX(Holderprofit.rowid) FROM Holderprofit)) OR ((SELECT COUNT(Holderprofit.rowid) FROM Holderprofit) = 0))")
    cur.execute("UPDATE Unfinishedtasks SET assigned = 0")
    unfinished = cur.execute("SELECT * FROM Unfinishedtasks LIMIT 1").fetchone()
    is_unfinished = len(unfinished)
    conn.commit()

    while is_unfinished:
        while threading.active_count() < 20:
            task = cur.execute("SELECT rowid FROM Unfinishedtasks WHERE assigned = 0 ORDER BY rowid ASC LIMIT 1 ").fetchone()
            
            if task is None: 
                is_unfinished = False
                break
            else:
                task_rowid = task[0]
                
            cur.execute("UPDATE Unfinishedtasks SET assigned = ? WHERE rowid = ? ", (1, task_rowid))
            conn.commit()
            
            print task
            
            threading.Thread(target=create_table_profit, args=(task_rowid, )).start()
            
    print 'Bingo!'
    
if __name__ == '__main__':    
    #create_table_profit()
    multi_thread(0, 0)
    
    if threading.active_count() <= 1:
            cur.close()
            conn.close()