from lxml import html
from lxml import etree
from bs4 import BeautifulSoup
import requests
import re
import pprint
import json
import time
import datetime

base_url = 'http://magic.wizards.com'
errorLog = []
finalStandingsLinks = []
finalStandingsTables = []

def get_links(soup):
  # Shortcut to extract links from any BeautifulSoup soup.
  # Returns a list of urls
  links = []
  for link in soup.find_all('a'):
    links.append(link.get('href'))
  return links

def parse_table(table):
  parsedTable = []
  rows = table.find_all('tr')
  #######
  headers = []
  if len(table.find_all('th')) == 0:
    for td in rows[0].find_all('td'):
      headers.append(td.contents[0])
  else:
    for th in table.find_all('th'):
      headers.append(th.contents[0])
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

def parseEventPage(session,url):
  # Takes a url to a Wizards coverage page.
  # Finds:
  #  * Name of the Event
  #  * Date of the Event
  #  * Type of Event (GP, PT, Invitational, Worlds, Masters, Other)
  #  * Link to Final Standings
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

#def main():
events = []
linkList = []
session = requests.Session()
timeout = datetime.timedelta(seconds=120)
for links in getCoverageLinks(getSeasons(session),start=3,end=4):
  for link in links:
    if link[0:4] != 'http':
      link = base_url + link
    linkList.append(link)
    starttime = datetime.datetime.now()
    event = {}
    while not 'name' in event:
      event = parseEventPage(session,link)
      time.sleep(1)
      print "Could not get event information, trying again..."
      if datetime.datetime.now() - starttime > timeout:
        print "Timeout.  Moving on..."
        break
    events.append(parseEventPage(session,link))

for link in finalStandingsLinks:
  url = base_url + link[1].attrs['href']
  finalStandingsTables.append([link[0],getData(url)])

with open('tables.json','w') as outfile:
  json.dump(finalStandingsTables,outfile)