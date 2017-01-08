import sqlite3
import re
from environment import *

def findnum(s) :
    pattern = re.compile("[+-]?\d+(?:\.\d+)?")
    return float(pattern.search(s).group(0))

def connect_db() :
    conn = sqlite3.connect(db_location)
    return conn

fl = 'TransHist.csv'
handle = open(fl)

dictionary = {}

for line in handle :
    if line not in dictionary :
        dictionary.update({line : 1})
    else :
        dictionary.update({line : dictionary[line] + 1})

for key, value in dictionary.items():
    columns = key.split(',')
    datum = columns[0]
    desc = columns[1]
    amount = findnum(columns[2])
    conn = connect_db()
    cur = conn.cursor()
    cur.execute('''SELECT count(id) FROM items WHERE datum = ? AND description = ? AND amount = ?''', (datum, desc, amount ))
    rowcount = cur.fetchone()[0]
    toinsert = value - rowcount
    if toinsert < 0 :
        cur.execute('''DELETE FROM items WHERE datum = ? AND description = ? AND amount = ? LIMIT ?''', (datum, desc, amount, toinsert * -1 ))
    else :
        for i in range(toinsert) :
            cur.execute('''INSERT INTO items(datum, description, amount) VALUES(?, ?, ?)''', (datum, desc, amount, ))
    conn.commit()
    conn.close()
