import json
import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.setLevel(logging.ERROR)

def isValidCuisine(cuisine):
    availableCuisines = {"mexican","chinese","japanese"}
    print(cuisine)
    if cuisine is not None and cuisine.lower() not in availableCuisines:
        return False
    
    return True

def isValidDate(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False

def buildInvalidResponse(isValid, violatedSlot, violationMessage):
    return {
        'isValid': isValid,
        'violatedSlot': violatedSlot,
        'message': {
            'contentType': 'PlainText',
            'content': violationMessage
        }
    }

def validateIntent(intent):
    cuisine = intent['Cuisine']
    date = intent['Date']
    time = intent['Time']
    
    if cuisine is not None and not isValidCuisine(cuisine):
        return buildInvalidResponse(
            False,
            'Cuisine',
            'We currently do not have suggestions for {}. Please try some other cuisine'.format(cuisine)
        )
    
    if date is not None and not isValidDate(date):
        return buildInvalidResponse(
            False,
            'Time',
            'I did not understand the date you entered, please enter a valid date'
        )
    if date is not None and datetime.datetime.strptime(date, '%Y-%m-%d').date() <= datetime.date.today():
        return buildInvalidResponse(
            False,
            'Date',
            'Reservations must be scheduled at least one day in advance. Can you try a different date?'
        )
        
    return {
        'isValid': True
    }

def elicitSlot(sessionAttributes, intentName, slots, slotToElicit, message):
    return {
        'sessionAttributes': sessionAttributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intentName,
            'slots': slots,
            'slotToElicit': slotToElicit,
            'message': message
        }
    }

def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response

def sendSuggestionsEmail(event):
    return True
    
def tryThis(func):
    try:
        return func
    except KeyError:
        return None

def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

def sendPreferencesToQueue(event):
    client = boto3.client("sqs")
    url = client.get_queue_url(QueueName='userPreferences').get('QueueUrl')
    cuisine = event['Cuisine']
    date = event['Date']
    time = event['Time']
    location = event['Location']
    numberOfPeople = event['NumberOfPeople']
    name = event['Name']
    email = event['Email']
    
    response = client.send_message(
        QueueUrl = url,
        MessageAttributes = {
            'cuisine':{
                'DataType': 'String',
                'StringValue': cuisine
            },
            'date':{
                'DataType': 'String',
                'StringValue': date
            },
            'time':{
                'DataType': 'String',
                'StringValue': time
            },
            'numberOfPeople':{
                'DataType': 'String',
                'StringValue': numberOfPeople
            },
            'location':{
                'DataType': 'String',
                'StringValue': location
            },
            'name':{
                'DataType': 'String',
                'StringValue': name
            },
            'email':{
                'DataType': 'String',
                'StringValue': email
            }
        },
        MessageBody = ("user"),
    )
    
    print(response)
    print("Queue sent")
    logger.debug("SQS messages sent!")


def getSuggestions(event):
    
    userCuisine = tryThis(event['currentIntent']['slots']['Cuisine'])
    userLocation = tryThis(event['currentIntent']['slots']['Location'])
    userNumberOfPeople = tryThis(event['currentIntent']['slots']['NumberOfPeople'])
    userDate = tryThis(event['currentIntent']['slots']['Date'])
    userTime = tryThis(event['currentIntent']['slots']['Time'])
    
    sessionAttributes = event['sessionAttributes'] if event['sessionAttributes'] is not None else {}
    
    reservation = json.dumps({
        'Location': userLocation,
        'Date': userDate,
        'Time': userTime,
        'Cuisine': userCuisine,
        'NumberOfPeople': userNumberOfPeople
    })
    
    sessionAttributes['currentReservation'] = reservation
    
    if event['invocationSource'] == 'DialogCodeHook':
        validationResult = validateIntent(event['currentIntent']['slots'])
        if not validationResult['isValid']:
            slots = event['currentIntent']['slots']
            slots[validationResult['violatedSlot']] = None
        
            return elicitSlot(
                sessionAttributes,
                event['currentIntent']['name'],
                slots,
                validationResult['violatedSlot'],
                validationResult['message']
            )
        # if userCuisine and userDate:
        #     flag = sendSuggestionsEmail(event)
            # sessionAttributes['currentReservationPrice'] = price
        # else:
        #     try_ex(lambda: session_attributes.pop('currentReservationPrice'))

        sessionAttributes['currentReservation'] = reservation
        return delegate(sessionAttributes, event['currentIntent']['slots'])
           
    if event['invocationSource'] == 'FulfillmentCodeHook':
        print("Inside fulfillmentState")
        sendPreferencesToQueue(event['currentIntent']['slots'])
     
    return close(
        sessionAttributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Thank you for the preferences. Please check your email for suggestions from us!'
        }
    )
            

# --- Intents ---


def dispatch(intent_request):
    # logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    print(intent_request)
    
    if intent_name == 'GreetingIntent':
        return {
            'dialogAction': {
                'type': 'Close',
                'fulfillmentState': 'Fulfilled',
                'message': {
                    'contentType': 'PlainText',
                    'content': 'Hi there! How can I help you today?'
                    
                }
            }
        }

    if intent_name == 'DiningSuggestionsIntent':
        return getSuggestions(intent_request)
        
        # return {
        #     'dialogAction': {
        #         'type': 'Close',
        #         'fulfillmentState': 'Fulfilled',
        #         'message': {
        #             'contentType': 'PlainText',
        #             'content': 'DiningSuggestionsIntent called by Sheena'
        #         }
        #     }
        # }

    if intent_name == 'ThankYouIntent':
        return {
            'dialogAction': {
                'type': 'Close',
                'fulfillmentState': 'Fulfilled',
                'message': {
                    'contentType': 'PlainText',
                    'content': 'It was great assisting you, hope you have a great time!'
                }
            }
        }

    raise Exception('Intent with name ' + intent_name + ' not supported')


# --- Main handler ---


def lambda_handler(event, context):
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    print(event)
    logger.debug(event)

    return dispatch(event)
