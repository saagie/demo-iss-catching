import time,schedule
import requests
import sys
import logging
import json
from datetime import datetime
from elasticsearch import Elasticsearch, RequestError
import os


log_format = "%(asctime)s::%(levelname)s::%(name)s::"\
             "%(filename)s::%(lineno)d::%(message)s"
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_format)

# The URL where we extract data

URL = "https://www.n2yo.com/rest/v1/satellite/positions/25544/41.702/-76.014/0/2/&apiKey="+os.environ['CHANDIMA_API'] 

# Connect to Elasticsearch

def connectES():
  es = Elasticsearch('http://'+os.environ['IP_ELASTICSEARCH']+':9200',timeout=600)
  logging.info('Connect to Elasticsearch: OK!!')
  return es

es = connectES()

# Create an index in Elasticsearch

def create_index():
  settings = { "settings": {
                 "number_of_shards":1,
                  'number_of_replicas':0
                 },
      "mappings" : { 
           "document" : {
                "properties":{
                    "geo": {
                       "type": "geo_point"
                            }
                          }
                        } 
                     } 
                  }
  if not es.indices.exists(index = 'spacestation'):
    es.indices.create(index = 'spacestation', body=settings)
  else:
    pass

# Data streaming and save to Elasticsearch index

def collect_data():
  try:
    data = requests.get(url = URL).json()
    create_index()
    del data['positions'][1]
    new_data = {'geo':{'lat':data['positions'][0]['satlatitude'],
               'lon':data['positions'][0]['satlongitude']}, 
                'satname': data['info']['satname'], 'satid': data['info']['satid'], 
                  'timestamp':datetime.fromtimestamp(data['positions'][0]['timestamp']).isoformat()}
    es.index(index='spacestation', doc_type='document', body=new_data)
  except:
    collect_data()

# Data streaming for every 10 seconds

schedule.every(10).seconds.do(collect_data)

while True:
  schedule.run_pending()
  time.sleep(1) 