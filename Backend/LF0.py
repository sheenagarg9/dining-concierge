import boto3
import json

def lambda_handler(event, context):
    client = boto3.client('lex-runtime')
    print(event)
    modEvent = json.loads(event['body'])
    print(modEvent)
    # This post_text function sends  user message to Lex and get backs the response from it
    response = client.post_text(botName='DiningConcierge', botAlias='$LATEST', userId='sheena',
                                inputText=modEvent['messages'][0]['unstructured']['text'])
    print(response)
    print(response['message'])

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*", 
            "Access-Control-Allow-Credentials": True,
            "Access-Control-Allow-Headers": "Content-Type", 
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        "body": json.dumps({
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "id": 1,
                        "text": response['message'],
                        "timestamp": "10-13-2023"
                    }
                }
            ]
        })
    }
