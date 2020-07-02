from elasticsearch import Elasticsearch
import sys
import json
import requests
import logging
from healthcheck import HealthCheck
import ast
from flask import Flask, jsonify, request
import numpy as np
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins='*', allow_headers='*')

logging.basicConfig(filename="flask.log", level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s")

def howami():
    return True, "I am good"

health = HealthCheck(app, "/hcheck")
health.add_check(howami)


def connect2ES(ipAddress='localhost', port=9200):
    # connect to ES
    es = Elasticsearch([{'host': ipAddress, 'port': port}])
    if es.ping():
            app.logger.info(f"Connected to ES!")
    else:
            app.logger.info(f"Could not connect to ES!")
            sys.exit()
    return es

# Search by Keywords
def keywordSearch(es, q):
    b={
        'query':{
            'multi_match':{
                "query": q,
                "fields": ["name", "description", "keywords"]
                }
            }
        }
    res= es.search(index='compositesearch',body=b)
    return res['hits']['hits']

def getResponseAgainstDoId(apiUrl, searchString, limit=1):
    headers = {"Content-Type" : "application/json", "Cache-Control": "no-cache"}
    data = {
      "request": {
         "filters": {
                     "status"     : ["Live"],
                     "channel"    : "DUMMY CHANNEL",
                     "board"      : ["DUMMY BOARD"],
                     "contentType": ["Collection", "TextBook", "LessonPlan", "Resource"],
                     },
         "limit"  : limit,
         "query"  : searchString,
         "softConstraints": {
                             "badgeAssertions" : 98,
                             "board"           : 99,
                             "channel"         : 100
                             },
         "mode"  :"soft",
         "facets": ['board', 'gradeLevel', 'subject', 'medium'],
         "offset": 0
      }
    }
    r = requests.post(url=apiUrl, json=data, headers=headers)
    if r.status_code == 200:
        return r.text
    else:
        return -1

# Search by Vec Similarity
def sentenceSimilaritybyNN(vecServerEndpoint, es, searchQuery):
    headers = {'Content-Type': 'application/json'}
    payload = {"searchString": [searchQuery]}
    response = requests.post(url=vecServerEndpoint, headers=headers, json=payload)
    query_vector = ast.literal_eval(response.text)[0]

    b = {"query" : {
                "script_score" : {
                    "query" : {
                        "bool" : {
                            "must": [{"match": {"status" : "Live"}}],
                            "filter" : {
                                      "match": {
                                            "contentType" : "Resource"
                                               }
                                       }
                                 }
                    },
                    "script" : {
                        "source": """
                                         cosineSimilarity(params.query_vector, 'name_vector') + 
                                         cosineSimilarity(params.query_vector, 'description_vector') + 
                                         1.0
                                  """,
                        "params": {"query_vector": query_vector}
                    }
                }
             }
        }

    res= es.search(index='compositesearch', body=b)
    return res['hits']['hits']

@app.route('/keywordsearch',  methods=['POST'])
def kwSearch():
    ESServer = request.args.get('ESServer') or request.get_json().get('ESServer', '')
    vecServerEndpoint = request.args.get('vecServerEndpoint') or request.get_json().get('vecServerEndpoint', '')
    searchQuery = request.args.get('searchQuery') or request.get_json().get('searchQuery', '')
    es = connect2ES(ipAddress=ESServer)

    results = keywordSearch(es, searchQuery)
    output = dict()
    for hit in results:
        output[str(hit['_score'])] = hit['_source']['name']
    return jsonify(output)


@app.route('/nlpsearch',  methods=['POST'])
def nlpsearch():
    ESServer = request.args.get('ESServer') or request.get_json().get('ESServer', '')
    vecServerEndpoint = request.args.get('vecServerEndpoint') or request.get_json().get('vecServerEndpoint', '')
    apiUrl = request.args.get('apiUrl') or request.get_json().get('apiUrl', '')
    #ESServer = "52.172.51.143"
    #vecServerEndpoint = "http://52.172.51.143:8890/getvector"
    #apiUrl = "apiUrl": "https://mahacyber.ddns.net/api/content/v1/search"

    searchString = request.args.get('searchString') or request.get_json().get('searchString', '')
    es = connect2ES(ipAddress=ESServer)

    results = sentenceSimilaritybyNN(vecServerEndpoint, es, searchString)
    #output = dict()
    output = []
    for hit in results:
        #output[str(hit['_score'])] = hit['_source']['name']
        #print(hit['_score'],  hit['_source']['name'])
        output.append(json.loads(getResponseAgainstDoId(apiUrl, hit['_source']['identifier'], limit=1)))
    #return jsonify(output)
    return {"responseCode": "OK", "result": output, 'error': '', 'count': len(output)}

@app.route('/advancesearch',  methods=['POST'])
def advanceSearch():
    return jsonify("DONE")

@app.route('/autocomplete',  methods=['POST'])
def autocomplete():
    ESServer = request.args.get('ESServer') or request.get_json().get('ESServer', '')
    QueryString = request.args.get('QueryString') or request.get_json().get('QueryString', '')
    es = connect2ES(ipAddress=ESServer)
    # search query
    b = {
          "size": 10,
          "_source": ["name", "mimeType", "contentType"], 
          "query": { 
            "bool": { 
              "must": [
                { "multi_match": {
                        "query": QueryString,
                        "fields": ["name"],
                        "analyzer": "standard",
                        "operator": "and",
                        "fuzziness": 1,
                        "fuzzy_transpositions": "true"
                        }},
                {
                  "match": {
                        "status" : "Live"
                  }
                }
              ],
              "filter": [
                {
                  "match": {
                    "contentType" : "Resource"
                  }
                }
              ]
            }
          }
        }
    resp = es.search(index="compositesearch", body=b)
    respList = [hit["_source"]["name"] for hit in resp["hits"]["hits"] if len(hit["_source"]["name"]) < 30]
    #return jsonify({"autocompleteList": sorted(respList, reverse=True)})
    return jsonify([{"title": i} for i in sorted(respList, reverse=True)])

@app.route('/spellcorrect',  methods=['POST'])
def spellcorrect():
    ESServer = request.args.get('ESServer') or request.get_json().get('ESServer', '')
    QueryString = request.args.get('QueryString') or request.get_json().get('QueryString', '')
    es = connect2ES(ipAddress=ESServer)
    # search query
    b = {
         "size": 1,
         "_source": ["name", "mimeType", "contentType"],
         "query": {
           "bool": {
             "must": [
               {
                 "match": {
                   "name": {
                     "query": QueryString,
                     "operator": "and",
                     "fuzziness": 1,
                     "fuzzy_transpositions": "true"
                   }
                 }
               },
               {
                 "match": {
                   "status" : "Live"
                 }
               }
             ],
             "filter": [
               {
                 "match": {
                   "contentType" : "Resource"
                 }
               }
             ]
           }
         }
        }
    resp = es.search(index="compositesearch", body=b)
    return jsonify({"spellcorrectList": [hit["_source"]["name"] for hit in resp["hits"]["hits"]]})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3456, debug=True)