import sqlite3

conn = sqlite3.connect('stock.sqlite3')
cur = conn.cursor()

cur.execute('INSERT INTO Majorholderinfo (shareholder, quantity, percentage, equitytype, holderid, stockid, uptodate, reportdate, holdersnumber, averageshares) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, )')

conn.commit()
cur.close()
conn.close()