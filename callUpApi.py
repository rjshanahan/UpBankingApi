import os
import sys
import requests
import json
import re
import time
import numpy as np
import pandas as pd
import urllib.parse
from pathlib import Path
import copy
from datetime import datetime, timedelta, timezone


class connectUpBanking():
    """
    Extract all account, transaction and category data from Up! Banking API.
    This class handles connections, requests and parsing responses.
    Args:
        url (str): endpoint URL for UpBanking API
        apiKey (str): secret API Key obtained through Up! developer portal (https://developer.up.com.au)        
    Returns:
        accountList (dict): JSON payload containing flattened account and transaction data.
    """
    def __init__(self,
                 awsConnectionDict
                ):
        self.awsConnectionDict = awsConnectionDict
        self.apiKey = awsConnectionDict["UPBANKING_API_KEY"]
        self.url = awsConnectionDict["UPBANKING_URL_BASE"]
        self.ACCOUNTS = awsConnectionDict["ACCOUNTS"]
        self.TRANSACTIONS = awsConnectionDict["TRANSACTIONS"]

    
    def getUpBanking(self, mySession, action, query):
        """
        GET call to UpBanking API with URL query params
        Args:
            mySession (obj): existing HTTP session
            action (str): endpoint action for UpBanking API
            query (dict or str): POST method will be called if dict/JSON string, else GET method with URL query params
        """
        if query is None:
            UpBanking_ENDPOINT = action
        else:
            UpBanking_ENDPOINT = "{URL_BASE}{UpBanking_ACTION}?{UpBanking_QUERY}".format(
                URL_BASE = self.url,
                UpBanking_ACTION = str(action),
                UpBanking_QUERY = str(query)
                # UpBanking_QUERY = str(urllib.parse.quote_plus(query))
            )

        UpBankingResponse = mySession.get(UpBanking_ENDPOINT,
                                     headers = {'Connection':'close'})
    

        if UpBankingResponse.ok:
            print('INFO: status code for GET call to UpBanking endpoint "{}" is {}'.format(UpBanking_ENDPOINT, str(UpBankingResponse.status_code)))

        elif UpBankingResponse.status_code == 422:
            print('INFO: attempting GET call again - response code 422 received')
            UpBankingResponse = mySession.get(UpBanking_ENDPOINT,
                                     headers = {'Connection':'close'})

        elif UpBankingResponse.status_code == 503:
            print('INFO: attempting GET call again - response code 503 received')
            RetryStatus = 503
            while RetryStatus == 503:
                time.sleep(60)
                UpBankingResponseRetry = mySession.get(UpBanking_ENDPOINT,
                                     headers = {'Connection':'close'})
                RetryStatus = UpBankingResponseRetry.status_code

        else:
            print('ERROR: for endpoint "{}" error code {} received for payload\n{} with reason:\n{}'.format(UpBanking_ENDPOINT, str(UpBankingResponse.status_code), str(query), str(UpBankingResponse.text)))
        
        return json.loads(UpBankingResponse.text)
        
        

    def postUpBanking(self, mySession, action, query):
        """
        POST call to UpBanking API with JSON payload
        Args:
            mySession (obj): existing HTTP session
            action (str): endpoint action for UpBanking API
            query (dict or str): POST method will be called if dict/JSON string, else GET method with URL query params
            """
        
        UpBanking_ENDPOINT = "{URL_BASE}{UpBanking_ACTION}".format(
            URL_BASE = self.url,
            UpBanking_ACTION = str(action)
        )

        UpBankingResponse = mySession.post(UpBanking_ENDPOINT,
                                      json = query,
                                      headers = {'Connection':'close'})

        if UpBankingResponse.ok:
            print('INFO: status code for POST call to UpBanking endpoint "{}" is {}'.format(UpBanking_ENDPOINT, str(UpBankingResponse.status_code)))

        elif UpBankingResponse.status_code == 422:
            print('INFO: attempting POST call again - response code 422 received')
            UpBankingResponse = mySession.post(UpBanking_ENDPOINT,
                                      json = query,
                                      headers = {'Connection':'close'})

        elif UpBankingResponse.status_code == 503:
            print('INFO: attempting POST call again - response code 503 received')
            RetryStatus = 503
            while RetryStatus == 503:
                time.sleep(60)
                UpBankingResponseRetry = mySession.post(UpBanking_ENDPOINT,
                                      json = query,
                                      headers = {'Connection':'close'})
                RetryStatus = UpBankingResponseRetry.status_code

        else:
            print('ERROR: for endpoint "{}" error code {} received for payload\n{} with reason:\n{}'.format(UpBanking_ENDPOINT, str(UpBankingResponse.status_code), str(query), str(UpBankingResponse.text)))
        
        return json.loads(UpBankingResponse.text)
    
        
    
    def callUpBanking(self, action, query):
        """Call UpBanking endpoint and return response data"""
        
        # create http session
        mySession = requests.Session()
        
        try:    
            # update headers with token
            UpBanking_HEADERS = {'Authorization': 'Bearer {}'.format(self.apiKey),
                        }

            # update headers with Authorisation
            mySession.headers.update(UpBanking_HEADERS)

            # trim non-mandatory headers
            for header in set(dict(mySession.headers).keys()) - set(['Authorization', 'Content-Type',]):
                del mySession.headers[header]
                
                
            # build query parameters and call UpBanking API
            try:
                postPayload = query if isinstance(query, dict) else json.loads(query)
                UpBankingResponse = self.postUpBanking(mySession, action, query)

            except (ValueError, TypeError):

                if query is None:
                    UpBankingResponse = self.getUpBanking(mySession, action, None)
                else:
                    queryString = '{QUERY}'.format(QUERY = str(query))
                    UpBankingResponse = self.getUpBanking(mySession, action, queryString)

    
        except (KeyError, ValueError, TypeError, requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError) as e:
            print('ERROR: calling UpBanking action: {} with type {} & reason: {}'.format(str(action), str(type(e)), str(e)))
            print('ERROR: request payload: {}'.format(str(query)))
            print('ERROR: response payload: {}'.format(str(UpBankingResponse.text)))
            UpBankingResponse = None
            
        return UpBankingResponse


    def categoryData(self, action, query):
        """
        Extract list of parent and child categories.
        Usage: categoryData = upYours.categoryData("/categories", "")
        """
        categoryData = self.callUpBanking(action, query)

        categoryList = []
        for category in categoryData["data"]:

            if category["relationships"]["parent"]["data"] == None:

                parentCategory = category["id"]
                
                if len(category["relationships"]["children"]["data"]) == 0:
                    categoryDict = {}
                    categoryDict["parentCategory"] = parentCategory
                    categoryDict["childCategory"] = None
                    categoryList.append(categoryDict)
                
                else:
                    for child in category["relationships"]["children"]["data"]:
                        categoryDict = {}
                        categoryDict["parentCategory"] = parentCategory
                        categoryDict["childCategory"] = child["id"]
                        categoryList.append(categoryDict)
            else:
                pass
        
        return categoryList



    def accountData(self, ):
        """
        Extract account and transaction data. Handling for transaction pagination includes.
        """
        # retrive account data
        accountData = self.callUpBanking(self.ACCOUNTS["action"], 
                                        self.ACCOUNTS["query"])

        accountList = []
        for account in accountData["data"]:
            
            accountTransactionDict = {}

            accountTransactionDict["accountId"] = account["id"]
            accountTransactionDict["accountName"] = account["attributes"]["displayName"]
            accountTransactionDict["accountType"] = account["attributes"]["accountType"]
            accountTransactionDict["accountBalance"] = float(account["attributes"]["balance"]["value"])

            # retrieve transaction data for account
            transactionData = self.callUpBanking(self.TRANSACTIONS["action"].format(accountId=accountTransactionDict["accountId"]), 
                                        self.TRANSACTIONS["query"])

            transactionDataRaw = transactionData["data"]

            # get next page
            while transactionData["links"]["next"] is not None:

                transactionData = self.callUpBanking(transactionData["links"]["next"], None)

                transactionDataRaw = transactionDataRaw + transactionData["data"]

            for transaction in transactionDataRaw:

                transactionDict = copy.deepcopy(accountTransactionDict)

                transactionDict["transactionStatus"] = transaction["attributes"]["status"]
                transactionDict["rawText"] = transaction["attributes"]["rawText"]
                transactionDict["description"] = transaction["attributes"]["description"]
                transactionDict["message"] = transaction["attributes"]["message"]
                transactionDict["transactionAmount"] = float(transaction["attributes"]["amount"]["value"])
                transactionDict["transactionPostedOn"] = transaction["attributes"]["createdAt"]
                transactionDict["transactionSettledOn"] = transaction["attributes"]["settledAt"]
                try:
                    transactionDict["category"] = transaction["relationships"]["category"]["data"]["id"]
                    transactionDict["parentCategory"] = transaction["relationships"]["parentCategory"]["data"]["id"]
                except TypeError:
                    transactionDict["category"] = None
                    transactionDict["parentCategory"] = None
                    pass

                accountList.append(transactionDict)

        return accountList



configurationDict = {
    "UPBANKING_API_KEY": "GET_YOUR_OWN",
    "UPBANKING_URL_BASE": "https://api.up.com.au/api/v1",
    "ACCOUNTS": {
        "action": "/accounts",
        "query": "page[size]=100"
    },
    "TRANSACTIONS": {
        "action": "/accounts/{accountId}/transactions",
        "query": "page[size]=100"
    },
}

# initialise class
upYours = connectUpBanking(configurationDict)

# convert to dataframe and output to local (Windows)
accountDataframe = pd.DataFrame(upYours.accountData())
accountDataframe.to_csv(r"{}/Documents/UpAccountTransactions.csv".format(Path.home()), index=False)


