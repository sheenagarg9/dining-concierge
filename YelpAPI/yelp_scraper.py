from datetime import datetime
from decimal import Decimal
import boto3
from botocore.exceptions import NoCredentialsError
import requests

# Set up the Yelp API endpoint and API key
url = "https://api.yelp.com/v3/businesses/search"
api_key = "*****************"
# Define your cuisine types
cuisine_types = ["Chinese","Mexican","Japanese","Indian","Italian"]

# Loop through each cuisine type and collect restaurants
restaurants = []
for cuisine in cuisine_types:
    params = {
        "term": cuisine+" restaurants",
        "location": "Manhattan, New York City",
        "limit":50  # Maximum limit per request is 1000
    }

    headers = {
        "Authorization":"Bearer ********************", 
        "accept":"application/json"
    }

    # print("test "+headers['Authorization'])

    response = requests.get(url, params=params, headers=headers)
    # print(response.json())
    
    # Check for successful response (status code 200)
    if response.status_code == 200:
        data = response.json()
        # Print the entire response for debugging
        # print(data)
        
        # Check if "businesses" key exists in the response
        if "businesses" in data:
            # Add unique restaurants to the list
            restaurants.extend(data["businesses"])
            for restaurant in data["businesses"]:
                restaurant["Cuisine"] = cuisine
                restaurants.append(restaurant)
        else:
            print("No 'businesses' key found in the response.")
    else:
        print(f"Error: {response.status_code}")

# Ensure no duplicates by using business IDs
unique_restaurants = {restaurant["id"]: restaurant for restaurant in restaurants}.values()
# print(unique_restaurants)


# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# Create a DynamoDB table if it doesn't exist
try:
    table = dynamodb.create_table(
        TableName='yelp-restaurants',
        KeySchema=[
            {
                'AttributeName': 'BusinessId',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'BusinessId',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    print("Table created successfully!")
except Exception as e:
    table = dynamodb.Table('yelp-restaurants')
    print("Table already exists. Using existing table.")

# Insert unique restaurants into DynamoDB
for restaurant in unique_restaurants:
    try:
        table.put_item(
            Item={
                'BusinessId': restaurant['id'],
                'Name': restaurant['name'],
                'Address': restaurant['location']['address1'],
                'Coordinates': {
                    'latitude': Decimal(str(restaurant['coordinates']['latitude'])),
                    'longitude': Decimal(str(restaurant['coordinates']['longitude']))
                },
                'NumberOfReviews': restaurant['review_count'],
                'Rating': Decimal(str(restaurant['rating'])),
                'ZipCode': restaurant['location']['zip_code'],
                'Cuisine':restaurant['Cuisine'],
                'InsertedAtTimestamp': str(datetime.now())  # Use proper timestamp format
            }
        )
        print(f"Inserted restaurant: {restaurant['name']}")
    except NoCredentialsError:
        print("No AWS credentials found.")
    except Exception as e:
        print(f"Error inserting restaurant {restaurant['name']}: {str(e)}")
