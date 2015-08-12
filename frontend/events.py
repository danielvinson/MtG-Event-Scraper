import json
import os
import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError, RqlDriverError
from flask import Flask, g, jsonify, render_template, request, abort

import frontend_utils

RDB_HOST =  os.environ.get('RDB_HOST') or 'localhost'
RDB_PORT = os.environ.get('RDB_PORT') or 28015
APP_DB = 'scraper'

app = Flask(__name__)
app.config.from_object(__name__)

@app.before_request
def before_request():
  try:
      g.rdb_conn = r.connect(host=RDB_HOST, port=RDB_PORT, db=APP_DB)
  except RqlDriverError:
      abort(503, "No database connection could be established.")

@app.teardown_request
def teardown_request(exception):
  try:
      g.rdb_conn.close()
  except AttributeError:
      pass

@app.route("/")
def show_page():
  eventCursor = r.table('events').run(g.rdb_conn)
  events = list(eventCursor)
  events = filterByName(events,'Ross, Tom')
  #events = getByName('Ross, Tom')
  return render_template('index.html', events=events, base_url='http://magic.wizards.com')

if __name__ == "__main__":
  app.run(host='0.0.0.0')