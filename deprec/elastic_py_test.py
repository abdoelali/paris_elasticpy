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



out_filepath = "/Users/aelali/Desktop/py_elasticsearch/test2"
log = logging.getLogger("elastic_py")
conn = elasticsearch.Elasticsearch()


the_query = {"query": {"match_all": {}}, "fields": ["id", "created_at", "user.friends_count", "user.followers_count", "user.lang","user.location", "text"], 'sort': { 'created_at': { 'order': 'asc' } }}

scanResp = conn.search( index="twitter", body=the_query, scroll='10s' )
scrollId = scanResp['_scroll_id']
doc_num = 1

response = conn.scroll( scroll_id = scrollId, scroll='10s')

with codecs.open(out_filepath, "wb") as f:

    csvf = unicodecsv.writer(f, encoding='utf-8')
    fieldnames = ["id", "created_at", "user.friends_count", "user.followers_count", "user.lang","user.location", "text"]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    while ( len( response['hits']['hits'] ) > 0 ):
        for item in response['hits']['hits']:
            print '\tDocument ' + str(doc_num) + ' of ' + str( response['hits']['total'] )
            doc_num += 1

           
            try:

                csvf.writerow(itertools.chain.from_iterable([item['fields']['id'], item['fields']['created_at'], item['fields']['user.friends_count'], item['fields']['user.followers_count'], item['fields']['user.lang'], item['fields']['user.location'], item['fields']['text']]))

            except KeyError:

                csvf.writerow(itertools.chain.from_iterable([item['fields']['id'], item['fields']['created_at'], ['0'], ['0'], item['fields']['user.lang'], ['0'], item['fields']['text']]))


                

        # end for item
        scrollId = response['_scroll_id']
        if doc_num >= response['hits']['total']:
            break
        response = conn.scroll( scroll_id = scrollId, scroll='10s' )
    # end of while
