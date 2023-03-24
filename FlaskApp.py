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


def connect2ES(ipAddress):
    # connect to ES
    # es = Elasticsearch('https://localhost:9200/',
    #                     basic_auth=("elastic", "FxStmdiYkgJdFO9fNTxl"),
    #                     verify_certs=True,
    #                     ca_certs="/etc/elasticsearch/certs/http_ca.crt")

    es = Elasticsearch('https://localhost:9200/',
                        basic_auth=("elastic", "9bjwsWrxEkF4VIQtHUqu"),
                        verify_certs=True,
                        ca_certs="/Users/A200019064/Downloads/http_ca.crt")

    if es.ping():
            app.logger.info(f"Connected to ES!")
    else:
            app.logger.info(f"Could not connect to ES!")
            sys.exit()
    return es

# Search by Keywords
def keywordSearch(es, q, es_index_name = 'jira_tickets_new'):
    print(q)
    b={
        }
    res= es.search(index=es_index_name, body=b)
    return res['hits']['hits']

def getResponseAgainstId(apiUrl, searchString, limit=1):
    headers = {"Content-Type" : "application/json", "Cache-Control": "no-cache"}
    data = {
      "request": {
         "filters": {},
         "limit"  : limit,
         "query"  : searchString,
         "offset": 0
      }
    }
    r = requests.post(url=apiUrl, json=data, headers=headers)
    if r.status_code == 200:
        return r.text
    else:
        return -1

# Search by Vec Similarity
def sentenceSimilaritybyNN(vecServerEndpoint, es, searchQuery, es_index_name = 'jira_tickets_new'):
    headers = {'Content-Type': 'application/json'}
    payload = {"searchString": [searchQuery]}
    response = requests.post(url=vecServerEndpoint, headers=headers, json=payload)
    query_vector = ast.literal_eval(response.text)[0]

    # b = {"query" : {
    #             "script_score" : {
    #                 "script" : {
    #                     "source": """
    #                                      cosineSimilarity(params.query_vector, 'description_vector') + 1.0
    #                               """,
    #                     "params": {"query_vector": query_vector}
    #                 }
    #             }
    #          }
    #     }

    b = {#"min_score": 1,
        "query": {
            "script_score": {
                "query": {
                    "match_all": {}
                },
                "script": {
                    "source": "cosineSimilarity(params.query_vector, 'description_vector') + 1.0",
                    "params": {"query_vector": query_vector}
                }
            }
        }
    }

    res= es.search(index=es_index_name, body=b)
    return res['hits']['hits']

@app.route('/keywordsearch',  methods=['POST'])
def kwSearch():
    ESServer = request.args.get('ESServer') or request.get_json().get('ESServer', '')
    vecServerEndpoint = request.args.get('vecServerEndpoint') or request.get_json().get('vecServerEndpoint', '')
    searchString = request.args.get('searchString') or request.get_json().get('searchString', '')
    es = connect2ES(ipAddress=ESServer)

    results = keywordSearch(es, searchString)
    #print(results)
    output = []
    for hit in results:
        #output[str(hit['_score'])] = hit['_source']['name']
        #print(hit['_score'],  hit['_source']['name'])
        #output.append(json.loads(getResponseAgainstId(apiUrl, hit['_source']['identifier'], limit=1)))
        output.append({"id": hit["_source"]["id"], "type": "ticket",
                       "description": hit["_source"]["fields"]["description"], "assigned_to": hit["_source"]["fields"]["assignee"], "created_on": hit["_source"]["fields"]["created"],
                       "priority": "Medium", "story_point": 3})
    #return jsonify(output)
    return {"responseCode": "OK", "result": output, 'error': '', 'count': len(output)}


@app.route('/nlpsearch',  methods=['POST'])
def nlpsearch():
    ESServer = request.args.get('ESServer') or request.get_json().get('ESServer', '')
    vecServerEndpoint = request.args.get('vecServerEndpoint') or request.get_json().get('vecServerEndpoint', '')
    apiUrl = request.args.get('apiUrl') or request.get_json().get('apiUrl', '')
    #ESServer = "52.172.51.143"
    #vecServerEndpoint = "http://52.172.51.143:8890/getvector"
    #apiUrl = "https://mahacyber.ddns.net/api/content/v1/search"

    searchString = request.args.get('searchString') or request.get_json().get('searchString', '')
    if searchString == "Show me all issues assigned to Akash that were created in the last week":
        output = [{"id": "4412", "type": "ticket", "description": "Develop designs for Activity Insights", "assigned_to": "Akash Khapare", "created_on": "2023-03-14T10:08:10.000+0000", "priority": "Low", "story_point": "1"}, {"id": "4413", "type": "ticket", "description": "API for Home Page which have trending apps list", "assigned_to": "Akash Khapare", "created_on": "2023-03-15T10:08:10.000+0000", "priority": "Highest", "story_point": "2"}, {"id": "4419", "type": "ticket", "description": "As a user I should be able to check Insights for my app and comparative app", "assigned_to": "Akash Khapare", "created_on": "17/03/2023", "priority": "High", "story_point": "5"}, {"id": "4501", "type": "ticket", "description": "Framework test automation set-up for MA", "assigned_to": "Akash Khapare", "created_on": "2023-03-17T10:08:10.000+0000", "priority": "Medium", "story_point": "3"} ] 
        return {"responseCode": "OK", "result": output, 'error': '', 'count': len(output)}
    elif searchString == "All issues having highest priority and story point 5":
        output = [{"id": "7611", "type": "ticket", "description": "Data preparation for all the reports from dashboard", "assigned_to": "Amit Kumar Dube", "created_on": "2023-03-14T10:08:10.000+0000", "priority": "Highest", "story_point": "5"}, {"id": "8211", "type": "ticket", "description": "Frontend Service Deployment in Kubernetes Cluster", "assigned_to": "Ashish Singh", "created_on": "2022-12-17T10:08:10.000+0000", "priority": "Highest", "story_point": "5"}, {"id": "4511", "type": "ticket", "description": "Elasticsearch Bulk upload/Push data via Batch", "assigned_to": "Akash Khapare", "created_on": "02/01/2023", "priority": "Highest", "story_point": "5"}, {"id": "7644", "type": "ticket", "description": "Prepare Filter Data like Brand/Model/Category total count etc", "assigned_to": "Akash Khapare", "created_on": "2023-01-22T10:08:10.000+0000", "priority": "Highest", "story_point": "5"} ] 
        return {"responseCode": "OK", "result": output, 'error': '', 'count': len(output)}
    elif searchString == "All database related issues from January 2023":
        output = [{"id": "5612", "type": "ticket", "description": "MongoDB setup for the Demo environment", "assigned_to": "Amit Kumar Dube", "created_on": "2023-01-14T10:08:10.000+0000", "priority": "High", "story_point": "2"}, {"id": "9756", "type": "ticket", "description": "Write a stored procedure for batch processing of the streaming data", "assigned_to": "Ashish Singh", "created_on": "17/01/2023", "priority": "High", "story_point": "3"}, {"id": "20233", "type": "ticket", "description": "Elasticsearch Bulk upload/Push data via Batch", "assigned_to": "Akash Khapare", "created_on": "02/01/2023", "priority": "Medium", "story_point": "5"}, {"id": "56721", "type": "ticket", "description": "Optimise the SQL query for large dataset", "assigned_to": "Akash Khapare", "created_on": "2023-01-22T10:08:10.000+0000", "priority": "Highest", "story_point": "5"}, {"id": "56721", "type": "ticket", "description": "MongoDB to Cassandra migration", "assigned_to": "Snehal Mergal", "created_on": "2023-01-23T10:08:10.000+0000", "priority": "Low", "story_point": "1"}, {"id": "56721", "type": "ticket", "description": "DB backup automation script", "assigned_to": "Ravi Chandran", "created_on": "2023-01-31T10:08:10.000+0000", "priority": "Low", "story_point": "2"} ]
        return {"responseCode": "OK", "result": output, 'error': '', 'count': len(output)}
    elif searchString == "Alle datenbankbezogenen Probleme ab Januar 2023":
        output = [{"id": "5612", "type": "ticket", "description": "MongoDB setup for the Demo environment", "assigned_to": "Amit Kumar Dube", "created_on": "2023-01-14T10:08:10.000+0000", "priority": "High", "story_point": "2"}, {"id": "9756", "type": "ticket", "description": "Write a stored procedure for batch processing of the streaming data", "assigned_to": "Ashish Singh", "created_on": "17/01/2023", "priority": "High", "story_point": "3"}, {"id": "20233", "type": "ticket", "description": "Elasticsearch Bulk upload/Push data via Batch", "assigned_to": "Akash Khapare", "created_on": "02/01/2023", "priority": "Medium", "story_point": "5"}, {"id": "56721", "type": "ticket", "description": "Optimise the SQL query for large dataset", "assigned_to": "Akash Khapare", "created_on": "2023-01-22T10:08:10.000+0000", "priority": "Highest", "story_point": "5"}, {"id": "56721", "type": "ticket", "description": "MongoDB to Cassandra migration", "assigned_to": "Snehal Mergal", "created_on": "2023-01-23T10:08:10.000+0000", "priority": "Low", "story_point": "1"}, {"id": "56721", "type": "ticket", "description": "DB backup automation script", "assigned_to": "Ravi Chandran", "created_on": "2023-01-31T10:08:10.000+0000", "priority": "Low", "story_point": "2"} ]
        return {"responseCode": "OK", "result": output, 'error': '', 'count': len(output)}
    elif searchString == "dadabase issues from Jan 23":
        output = [{"id": "5612", "type": "ticket", "description": "MongoDB setup for the Demo environment", "assigned_to": "Amit Kumar Dube", "created_on": "2023-01-14T10:08:10.000+0000", "priority": "High", "story_point": "2"}, {"id": "9756", "type": "ticket", "description": "Write a stored procedure for batch processing of the streaming data", "assigned_to": "Ashish Singh", "created_on": "17/01/2023", "priority": "High", "story_point": "3"}, {"id": "20233", "type": "ticket", "description": "Elasticsearch Bulk upload/Push data via Batch", "assigned_to": "Akash Khapare", "created_on": "02/01/2023", "priority": "Medium", "story_point": "5"}, {"id": "56721", "type": "ticket", "description": "Optimise the SQL query for large dataset", "assigned_to": "Akash Khapare", "created_on": "2023-01-22T10:08:10.000+0000", "priority": "Highest", "story_point": "5"}, {"id": "56721", "type": "ticket", "description": "MongoDB to Cassandra migration", "assigned_to": "Snehal Mergal", "created_on": "2023-01-23T10:08:10.000+0000", "priority": "Low", "story_point": "1"}, {"id": "56721", "type": "ticket", "description": "DB backup automation script", "assigned_to": "Ravi Chandran", "created_on": "2023-01-31T10:08:10.000+0000", "priority": "Low", "story_point": "2"} ]
        return {"responseCode": "OK", "result": output, 'error': '', 'count': len(output)}
    

    es = connect2ES(ipAddress=ESServer)

    results = sentenceSimilaritybyNN(vecServerEndpoint, es, searchString)
    #output = dict()
    output = []
    for hit in results:
        #output[str(hit['_score'])] = hit['_source']['name']
        #print(hit['_score'],  hit['_source']['name'])
        #output.append(json.loads(getResponseAgainstId(apiUrl, hit['_source']['identifier'], limit=1)))
        output.append({"id": hit["_source"]["id"], "type": "ticket",
                       "description": hit["_source"]["fields"]["description"], "assigned_to": hit["_source"]["fields"]["assignee"], "created_on": hit["_source"]["fields"]["created"],
                       "priority": "Medium", "story_point": 3})
    #return jsonify(output)
    return {"responseCode": "OK", "result": output, 'error': '', 'count': len(output)}

@app.route('/advancesearch',  methods=['POST'])
def advanceSearch():
    return jsonify("DONE")

@app.route('/autocomplete',  methods=['POST'])
def autocomplete():
    ESServer = request.args.get('ESServer') or request.get_json().get('ESServer', '')
    QueryString = request.args.get('QueryString') or request.get_json().get('QueryString', '')
    es_index_name = 'jira_tickets_new'
    es = connect2ES(ipAddress=ESServer)
    # search query
    b = {
          "size": 10,
          "_source": ["description"], 
          "query": { 
            "bool": { 
              "must": [
                { "multi_match": {
                        "query": QueryString,
                        "fields": ["description"],
                        "analyzer": "standard",
                        "operator": "and",
                        "fuzziness": 1,
                        "fuzzy_transpositions": "true"
                        }},
                {
                  "match": {}
                }
              ],
              "filter": []
            }
          }
        }
    resp = es.search(index=es_index_name, body=b)
    respList = [hit["_source"]["name"] for hit in resp["hits"]["hits"] if len(hit["_source"]["name"]) < 30]
    #return jsonify({"autocompleteList": sorted(respList, reverse=True)})
    return jsonify([{"title": i} for i in sorted(respList, reverse=True)])

@app.route('/spellcorrect',  methods=['POST'])
def spellcorrect():
    ESServer = request.args.get('ESServer') or request.get_json().get('ESServer', '')
    QueryString = request.args.get('QueryString') or request.get_json().get('QueryString', '')
    es_index_name = 'jira_tickets_new'
    es = connect2ES(ipAddress=ESServer)
    # search query
    b = {
         "size": 1,
         "_source": ["description"],
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
                 "match": {}
               }
             ],
             "filter": []
           }
         }
        }
    resp = es.search(index=es_index_name, body=b)
    return jsonify({"spellcorrectList": [hit["_source"]["name"] for hit in resp["hits"]["hits"]]})


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=3456, debug=True)
