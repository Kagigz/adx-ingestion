import requests 
import logging
import json

def getRequest(endpoint,headers={}):
    response = ""
    try:
        req =  requests.get(url = endpoint, headers = headers) 
        logging.info("GET request processed successfully: %s"%endpoint)
        result = req.text
        logging.info("GET request result: %s " % result)
        response = json.loads(result)
    except Exception as e:
        logging.error("Could not process GET request: %s"%endpoint)
        logging.error(e)
    return response

def postRequest(endpoint, payload):
    result = ""
    try:
        headers = {"Content-type": "application/json"}
        req =  requests.post(url = endpoint, json = payload, headers = headers) 
        logging.info("POST request processed successfully: %s"%endpoint)
        result = req.text 
        logging.info("POST request result: %s " % result)
    except Exception as e:
        logging.error("Could not process POST request: %s"%endpoint)
        logging.error(e)
    return result
