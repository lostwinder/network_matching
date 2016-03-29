#!/usr/bin/python
import elasticsearch
es = elasticsearch.Elasticsearch()

cms_index_list = [\
  'logstash-2016.02.21',
  'logstash-2016.02.22',
  'logstash-2016.02.23',
  'logstash-2016.02.24',
  'logstash-2016.02.25',
  'logstash-2016.02.26',
  'logstash-2016.02.28',
  'logstash-2016.02.29',
  'logstash-2016.03.01',
  'logstash-2016.03.02',
  'logstash-2016.03.03',
  'logstash-2016.03.04',
  'logstash-2016.03.05',
  'logstash-2016.03.06',
  'logstash-2016.03.07',
  'logstash-2016.03.08'
]

GlobalJobId_dict = set()

for index_name in cms_index_list:
  page = es.search(
    index = index_name,
    doc_type = 'cms_production',
    scroll = '2m',
    search_type = 'scan',
    size = 1000,
    body = {"query":{"match_all":{}},"fields":["_id"]})

  sid = page['_scroll_id']
  scroll_size = page['hits']['total']
  print page
  print "scroll size: " + str(scroll_size)
  # start scrolling
  while (scroll_size > 0):
    page = es.scroll(scroll_id = sid, scroll = '2m')
    sid = page['_scroll_id']
    scroll_size = len(page['hits']['hits'])
    ids = [d['_id'] for d in page['hits']['hits']]
    print len(ids)
    print "scroll size: " + str(scroll_size)
    for id in ids:
      res = es.search(index=index_name, body={"query":{"term":{"_id":id}}, "size":1, "fields":["GlobalJobId"]})
      try:
        global_job_id = res['hits']['hits'][0]['fields']['GlobalJobId'][0]
        if global_job_id in GlobalJobId_dict:
          # delete this document
          es.delete(index=index_name, doc_type="cms_production", id=id)
          print "duplicate documentation with id: " + global_job_id
        else:
          GlobalJobId_dict.add(global_job_id)
      except KeyError:
        pass
