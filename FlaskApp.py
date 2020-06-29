from elasticsearch import Elasticsearch
import sys
import requests
import logging
from healthcheck import HealthCheck
import ast
from flask import Flask, jsonify, request
import numpy as np

app = Flask(__name__)

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

# Search by Vec Similarity
def sentenceSimilaritybyNN(vecServerEndpoint, es, searchQuery):
    headers = {'Content-Type': 'application/json'}
    payload = {"searchString": [searchQuery]}
    response = requests.post(url=vecServerEndpoint, headers=headers, json=payload)
    query_vector = ast.literal_eval(response.text)[0]

    b = {"query" : {
                "script_score" : {
                    "query" : {
                        "match_all": {}
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


@app.route('/semanticsearch',  methods=['POST'])
def semSearch():
    ESServer = request.args.get('ESServer') or request.get_json().get('ESServer', '')
    vecServerEndpoint = request.args.get('vecServerEndpoint') or request.get_json().get('vecServerEndpoint', '')
    searchQuery = request.args.get('searchQuery') or request.get_json().get('searchQuery', '')
    es = connect2ES(ipAddress=ESServer)

    results = sentenceSimilaritybyNN(vecServerEndpoint, es, searchQuery)
    output = dict()
    for hit in results:
        output[str(hit['_score'])] = hit['_source']['name']
    return jsonify(output)

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
          "size": 5,
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
    respList = [hit["_source"]["name"] for hit in resp["hits"]["hits"]]
    return jsonify({"autocompleteList": sorted(respList, reverse=True)})

@app.route('/spellcorrect',  methods=['POST'])
def spellcorrect():
    ESServer = request.args.get('ESServer') or request.get_json().get('ESServer', '')
    QueryString = request.args.get('QueryString') or request.get_json().get('QueryString', '')
    es = connect2ES(ipAddress=ESServer)
    # search query
    b = {
         "size": 5,
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
    app.run(host='0.0.0.0', port=3999, debug=True)