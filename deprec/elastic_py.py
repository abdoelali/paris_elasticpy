import elasticsearch
import json
import elasticsearch.client
import StringIO
import os
import codecs
import unicodecsv
import logging
import datetime
import itertools
import csv

out_filepath = "/Users/aelali/Desktop/py_elasticsearch/test"
log = logging.getLogger("elastic_py")
es = elasticsearch.Elasticsearch()

res = es.search(
    index= "twitter", 
    body={"query": {"match_all": {}}, "size": 10000, "fields": ["id", "created_at", "user.friends_count", "user.followers_count", "user.lang","user.location", "text"]}, request_timeout=50)

# fields = [d['fields'] for d in res['hits']['hits']]
# ids = [d['_id'] for d in res['hits']['hits']]
# tweets = [d['text'] for d in fields]

# t = json.dumps(tweets, ensure_ascii=False).encode('utf8')


with codecs.open(out_filepath, "wb") as f:
    
	csvf = unicodecsv.writer(f, encoding='utf-8')
	fieldnames = ["id", "created_at", "user.friends_count", "user.followers_count", "user.lang","user.location", "text"]
	writer = csv.DictWriter(f, fieldnames=fieldnames)
	writer.writeheader()
	
	for h in res['hits']['hits']:
		csvf.writerow(itertools.chain.from_iterable([h['fields']['id'], h['fields']['created_at'], h['fields']['user.friends_count'], h['fields']['user.followers_count'], h['fields']['user.lang'], h['fields']['user.location'], h['fields']['text']]))
		

# print json.dumps(fields, ensure_ascii=False).encode('utf8')
