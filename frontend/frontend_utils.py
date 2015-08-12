def filterEventsByName(eventList, name):
  # Shows only events which a certain player played in
  results = []
  for event in eventList:
    for item in event['results']:
      if 'Player' in item:
        if item['Player'] == name:
          results.append(event)
      if 'Player Name' in item:
        if item['Player Name'] == name:
          results.append(event)
      if 'Name' in item:
        if item['Name'] == name:
          results.append(event)
  return results

def filterResultsByName(eventList, name):
  # Shows only results for one player
  results = []
  for event in eventList:
    for item in event['results']:
      if 'Player' in item:
        if item['Player'] == name:
          results.append(item)
      if 'Player Name' in item:
        if item['Player Name'] == name:
          results.append(item)
      if 'Name' in item:
        if item['Name'] == name:
          results.append(item)
  return {'results':results}