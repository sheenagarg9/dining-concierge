import json
import logging
import boto3
import requests
import random
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.setLevel(logging.ERROR)

def getRecommendationsfromElasticSearch(cuisine):
    es_host='search-restaurants-j4n4wnnv7sqt2oujqvytwfeara.us-east-1.es.amazonaws.com'
    # index_path = '/<index_name>/_doc/1/' 
    region = 'us-east-1' 
    service = 'es'

    # credentials = boto3.Session().get_credentials()
    awsauth = ("*********", "********")
    url = 'https://'+es_host
    
    # test ES connection
    # response = requests.get(url,auth=auth)
    # return response.content
    if cuisine == 'chinese':
        cuisine = 'Chinese'
    elif cuisine == 'mexican':
        cuisine = 'Mexican'
    elif cuisine == 'japanese':
        cuisine = 'Japanese'
        
    url = f'https://{es_host}/restaurant/_search'
    query = {
        "query": {
            "match": {
                "Cuisine": cuisine
            }
        }
    }
    headers = {
        "Content-Type": "application/json"
    }
    print(query)
    response = requests.get(url,auth=awsauth, headers=headers, data=json.dumps(query))
    res = response.json()
    logger.debug(res)
    print(res)
    noOfHits = res['hits']['total']
    logger.debug(noOfHits)
    hits = res['hits']['hits']
    businessId = []
    print(hits)

    for hit in hits:
        businessId.append(str(hit['_source']['BusinessId']))
    return random.sample(businessId,3)
    
    # return {
    #     'statusCode': 200,
    #     'body': 'Indexes fetched'
    # }
    
def getRecommendationsFromDb(elSearchIds):
    client = boto3.resource('dynamodb')
    table = client.Table('yelp-restaurants')
    res = []
    
    for id in elSearchIds:
        response = table.query(KeyConditionExpression=Key('BusinessId').eq(id))
        
        res.append(response)
        print(res)
        reccos = []
        for val in res:
            reccos.append([val['Items'][0]['Name'],val['Items'][0]['Address']])
    
    return reccos
    
def sendEmail(recommendations, userPreference):
    client = boto3.client('ses', region_name='us-east-1')
    SENDER = 'sg7394@nyu.edu'
    RECIPIENT = userPreference['email']['stringValue']
    SUBJECT = 'Restaurant Recommendations!'
    
    name = userPreference['name']['stringValue']
    cuisine = userPreference['cuisine']['stringValue']
    location = userPreference['location']['stringValue']
    date = userPreference['date']['stringValue']
    time = userPreference['time']['stringValue']
    numberOfPeople = userPreference['numberOfPeople']['stringValue']
    
    MESSAGE = "Hi {}, \n Here are some {} restaurant recommendations in {} as you asked for {} at {} for {} people! \n\n 1.{} at {} \n\n 2.{} at {} \n\n 3.{} at {} \n\n Hope you have a good time!".format(name, cuisine, location, date, time, numberOfPeople, str(recommendations[0][0]),str(recommendations[0][1]),str(recommendations[1][0]),str(recommendations[1][1]),str(recommendations[2][0]),str(recommendations[2][1]))

    response = client.send_email(
        Destination={
            'ToAddresses': [
                RECIPIENT,
            ],
        },
        Message={
            'Body': {
                'Text': {
                    'Data': MESSAGE,
                }
            },
            'Subject': {
                'Data': SUBJECT,
            },
        },
        Source=SENDER
    )
    
    return response
    
def lambda_handler(event, context):
    # TODO implement
    userPreference = event['Records'][0]['messageAttributes']
    cuisine = userPreference['cuisine']['stringValue']
    location = userPreference['location']['stringValue']
    
    elSearchIds = getRecommendationsfromElasticSearch(cuisine)
    recommendations = getRecommendationsFromDb(elSearchIds)
    
    response = sendEmail(recommendations, userPreference)
    logger.debug(event)
    return {
        'statusCode': 200,
        'body': 'Email Sent with the Recommendations! MessageID:{}'.format(response)
    }
