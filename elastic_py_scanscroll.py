
import elasticsearch
from elasticsearch import helpers
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

out_filepath = "/Users/aelali/Desktop/test_elastic"
log = logging.getLogger("elastic_py")
es = elasticsearch.Elasticsearch()

query = {"filter": {  "range": { "created_at": { "from": "Sun Nov 15 17:54:50 +0000 2015" } } }, "query": {"match_all": {}}, "fields": ["id", "created_at", "user.friends_count", "user.followers_count", "user.lang","user.location", "text", "coordinates.coordinates", "entities.hashtags.text"], "from" : 10, 'sort': { 'created_at': { 'order': 'asc' } }}
# query = {"filter": {  "range": { "created_at": { "from": "Sun Nov 15 17:54:50 +0000 2015" } } }, "query": {"match_all": {}}, "fields": ["id", "created_at", "user.friends_count", "user.followers_count", "user.lang","user.location", "text", "coordinates.coordinates", "entities.hashtags.text"], "from" : 10, 'sort': { 'created_at': { 'order': 'asc' } }}




res = es.search(
    index= "twitter",
    scroll = '10m',
    # size=1000,
    # search_type = 'scan',
    body=query)

sid = res['_scroll_id']


doc_num = 0

response= es.scroll(scroll_id=sid, scroll= "10m")



with codecs.open(out_filepath, "wb") as f:

	csvf = unicodecsv.writer(f, encoding='utf-8')
	fieldnames = ["id", "created_at","user.friends_count", "user.followers_count", "user.lang","user.location", "text", "long", "lat", "hashtags"]
	writer = csv.DictWriter(f, fieldnames=fieldnames)
	writer.writeheader()

	while ( len( response['hits']['hits'] ) > 0 ):

		for h in response['hits']['hits']:

			doc_num += 1
			print response
			print '\tDocument ' + str(doc_num) + ' of ' + str( response['hits']['total'] )

	   		try:
				lon = [h["fields"]["coordinates.coordinates"][0]]

			except KeyError:
				lon = ['NA']

			try:
				lat = [h["fields"]["coordinates.coordinates"][1]]

			except KeyError:
				lat = ['NA']

			try:
				hashtags = [h["fields"]["entities.hashtags.text"]]

			except KeyError:
				hashtags = ['NA']

	   		try:
				friendcount = h["fields"]["user.friends_count"]

			except KeyError:
				friendcount = ['NA']

			try:
				followercount = h["fields"]["user.followers_count"]

			except KeyError:
				followercount = ['NA']

			try:
				userlang = h["fields"]["user.lang"]

			except KeyError:
				userlang = ['NA']

			try:
				userlocation = h["fields"]["user.location"]

			except KeyError:
				userlocation = ['NA']

			csvf.writerow(itertools.chain.from_iterable([h['fields']['id'], h['fields']['created_at'], friendcount, followercount, userlang, userlocation, h['fields']['text'], lon, lat, hashtags]))

		scrollId = response['_scroll_id']
		if doc_num >= response['hits']['total']:
		    break
		# if doc_num == 1000:
		# 	break
		response = es.scroll( scroll_id = scrollId, scroll='10s' )


#########################################################################################

# scroll_size = res['hits']['total']
# body={"query": {"match_all": {}}, "fields": ["id", "created_at", "user.friends_count", "user.followers_count", "user.lang","user.location", "text"]}
# fields = [d['fields'] for d in res['hits']['hits']]
# result = elasticsearch.helpers.scan(es, index='twitter', query=body, doc_type="tweets", size=100000)
# print result
# ids = [d['_id'] for d in res['hits']['hits']]
# tweets = [d['text'] for d in fields]
# t = json.dumps(tweets, ensure_ascii=False).encode('utf8')


