from lxml import html
from lxml import etree
from bs4 import BeautifulSoup
import requests
import re
import pprint
import json
import time
import datetime
import os
import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError, RqlDriverError

## Schema ########################################
#
# (External)
# events
#   event
#     name
#     date
#     format
#     type (ProTour, GrandPrix, Worlds, Other)
#     results
#       Name
#       Finish/Rank
#       Country
#       Pro Points
#       Prize Money
#
# (Internal)
# links
#   link
#
##################################################

base_url = 'http://magic.wizards.com'
errorLog = []

RDB_HOST =  os.environ.get('RDB_HOST') or 'localhost'
RDB_PORT = os.environ.get('RDB_PORT') or 28015
APP_DB = 'scraper'

def initDB():
  # Create the Database for the App.
  # Run once.
  connection = r.connect(host=RDB_HOST, port=RDB_PORT)
  try:
    r.db_create(APP_DB).run(connection)
    print "Database %s Created Successfully" % APP_DB
    r.db(APP_DB).table_create('events').run(connection)
    print "Table events Added to %s" % APP_DB
    r.db(APP_DB).table_create('links').run(connection)
    print "Table links Added to %s" % APP_DB
  except:
    print "Database failed to create.  Perhaps it already exists?"
  finally:
    connection.close()

def addEventToDB(event):
  try:
    connection = r.connect(host=RDB_HOST, port=RDB_PORT, db=APP_DB)
  except RqlDriverError:
    abort(503, "No database connection could be established.")
  r.table('events').insert(event).run(connection)
  return True

def get_links(soup):
  # Shortcut to extract links from any BeautifulSoup soup.
  # Returns a list of urls
  links = []
  for link in soup.find_all('a'):
    links.append(link.get('href'))
  return links

def getSeasons(session):
  start_url = '/en/events/coverage'
  page = session.get(base_url + start_url)
  soup = BeautifulSoup(page.text)
  return soup.find_all('div', { 'class': 'bean_block bean_block_html bean--wiz-content-html-block '})

def getCoverageLinks(seasons, **kwargs):
  #
  # Returns a list of coverage links from the given seasons.
  #
  #
  start = kwargs.get('start', 0)
  end = kwargs.get('end',len(seasons))
  links = []
  for season in seasons[start:end]:
    links.append(get_links(season))
  return links

def parse_table(table):
  parsedTable = []
  rows = table.find_all('tr')
  #######
  headers = []
  if len(table.find_all('th')) == 0:
    for td in rows[0].find_all('td'):
      headers.append(td.getText())
  else:
    for th in table.find_all('th'):
      headers.append(th.getText())
  #######
  # print headers
  #######
  for row in rows[1:]:
    parsedRow = []
    for i in range(len(headers)):
      header = headers[i]
      try:
        value = row.find_all('td')[i].get_text()
        if value != '':
          parsedRow.append({header: value})
      except Exception, e:
        print e
        continue
    if len(parsedRow) > 0:
      parsedTable.append(parsedRow)
  return parsedTable

def getData(url):
  try:
    standingsPage = requests.get(url)
    standingsSoup = BeautifulSoup(standingsPage.text)
    return parse_table(standingsSoup.table)
  except Exception, e:
    raise Exception("Error getting standingsPage: %s" % e)

def parseEventPage(session,url):
  # Takes a url to a Wizards coverage page.
  # Finds:
  #  * Name of the Event
  #  * Date of the Event
  #  * Type of Event (GP, PT, Invitational, Worlds, Masters, Other)
  #  * Link to Final Standings
  #
  event = {}
  try:
    eventPage = requests.get(url)
  except Exception, e:
    errorLog.append([datetime.datetime.now(),e, url])
    return e
  eventSoup = BeautifulSoup(eventPage.text)
  print "Parsing page: "+eventSoup.title.get_text()
  if len(eventSoup.find_all('div', {'id': 'glow'})) > 0:
    pageStyle = 'magazine'
    #
    #
  elif 'sideboard' in url:
    pageStyle = 'sideboard'
    #
    #
  else:
    # "New" style of coverage page.
    pageStyle = 'new'
    event['name'] = eventSoup.h2.contents[0].contents[0]
    event['date'] = eventSoup.find_all('span',{'class': 'date'})[0].contents[0]
    if 'GRAND PRIX' in eventSoup.title.contents[0]:
      event['type'] = 'Grand Prix'
    elif 'PRO TOUR' in eventSoup.title.contents[0]:
      event['type'] = 'Pro Tour'
    elif 'WORLD CHAMPIONSHIP' in eventSoup.title.contents[0]:
      event['type'] = 'World Championship'
    else:
      event['type'] = 'Other'
    # Find final standings link
    resultsBlock = eventSoup.find_all('div', { 'class': 'bean_block bean_block_html bean--wiz-content-html-block '})
    for block in resultsBlock:
      try: 
        if 'FINAL STANDINGS' in block.a.contents[0].strip():
          print "Found Final Standings!"
          event['link'] = block.a.attrs['href']
      except Exception, e:
        errorLog.append([datetime.datetime.now(),e, block])
        try:
          if 'FINAL STANDINGS' in block.a.contents[0].contents[0].strip():
            print "Found Final Standings!"
            event['link'] = block.a.attrs['href']
        except Exception, e:
          errorLog.append([datetime.datetime.now(),e, block])
    if not 'link' in event:
      print 'Final Standings Not Found'
  return event

def getEventLinks(session):
  connection = r.connect(host=RDB_HOST, port=RDB_PORT, db=APP_DB)
  for section in getCoverageLinks(getSeasons(session)):
    for link in section:
      if link[0:4] != 'http':
        link = base_url + link
      # Check if link is already in database
      duplicate = False
      cursor = r.table('links').run(connection)
      for doc in cursor:
        if doc['location'] == link:
          duplicate = True
          print "Link %s already in Database" % link
          break
      # Add link to DB (link)
      if duplicate == False:
        r.table('links').insert({'location': link}).run(connection)
        print "%s added to Database" % link

def getEvents(session):
  connection = r.connect(host=RDB_HOST, port=RDB_PORT, db=APP_DB)
  linkCursor = r.table('links').run(connection)
  links = list(linkCursor)
  for document in links[500:]:
    print "--------------------------------------"
    print "--------------------------------------"
    print "--------------------------------------"
    print "--------------------------------------"
    eventLink = document['location']
    # Probably need retry logic here?  Maybe in parseEventPage?
    try:
      print "trying url: %s" % eventLink
      event = parseEventPage(session,eventLink)
      print "-- Event: --"
      print event
      print "------------"
    except:
      print "Failed to get Event Page"
    # Now that we have the page parsed, check for duplicate, then if not, add it to the database.
    print "Page parsed."
    try:
      if event['name'] and event['link'] != "":
        try:
          eventCursor = r.table('events').run(connection)
          duplicate = False
          for document in eventCursor:
            if event['name'] == document['name']:
              print "Duplicate event %s not added." % event
              duplicate = True
          if duplicate == False:
            print event
            addEventToDB(event)
            print "Event added to Database: %s" % event
        except Exception, e:
          print "Error adding to DB, likely parser cannot handle this type of page."
          print e
    except Exception, e:
      print "No Event Data?"
      print e

def getEventData():
  connection = r.connect(host=RDB_HOST, port=RDB_PORT, db=APP_DB)
  eventCursor = r.table('events').run(connection)
  events = list(eventCursor)
  for event in events:
    try:
      if event['link']:
        print "Link Found"
    except:
      print event
      print "No Link for this event, moving on."
    if event['link'][0:4] != 'http':
      url = base_url + event['link']
    else:
      url = event['link']
    try:
      data = getData(url)
    except Exception, e:
      print "Error parsing data", e
    print "Updating Table for %s" % event
    result = r.table('events').get(event['id']).update({'results':data}).run(connection)
    print result

if __name__ == "__main__":
  session = requests.Session()
  getEventLinks(session)
  getEvents(session)