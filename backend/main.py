import sys
import scraper
import database
import pprint
import sqlite3
from tinydb import TinyDB, where

base_url = 'http://magic.wizards.com'


db = TinyDB('pw.json')







db = sqlite3.connect('pw.db')
c = db.cursor()
c.execute('SELECT * FROM events')
for event in c.fetchall():
  print event

test = getData(base_url + c.fetchall()[27][3])

for link in finalStandingsLinks:
  url = base_url + link[1].attrs['href']
  finalStandingsTables.append([link[0],getData(url)])

with open('tables.json','w') as outfile:
  json.dump(finalStandingsTables,outfile)