import json
import boto3
import logging
import os
import requests

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

num_messages_to_get = 10

def send_email(message, email):
    logger.info('sending email')
    ses = boto3.client('ses')
    ses_response = ses.send_email(
    Source='abirbhavdutta@gmail.com',
    Destination={
        'ToAddresses': [
            email,
        ],
        'CcAddresses': [
            'abirbhavdutta@gmail.com'
        ]
    },
    Message={
        'Subject': {
            'Data': 'Your dining suggestions'
        },
        'Body': {
            'Text': {
                'Data': message
            }
        }
    }
    )
    logger.info(f'ses response is {ses_response}')
    
    
def build_message(dynamo_data, cuisine, num_people, location, dining_date, dining_time):
    message = [f'Your suggestions for {cuisine} cuisine for {num_people} people in {location}, at {dining_time} on {dining_date} are as follows: \n']
    num_restaurants = 1
    for item_data in dynamo_data:
        if num_restaurants > 3:
            break
        temp_message = [f'{num_restaurants}. ']
        item = item_data['Item']
        temp_message.append(f"Name: {item['Name']}, ")
        temp_message.append(f"Address: {item['Address']}\n")
        message += temp_message
        num_restaurants+=1
    return ''.join(message)

def get_dynamo_data(businessIds):
    data = []
    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table('yelp-restaurants')
    for businessId in businessIds:
        dynamo_response = table.get_item(Key={'BusinessId': businessId})
        data.append(dynamo_response)
    return data

def get_data_from_elastic_search(cuisine):
    businessIds = []
    
  
    OPEN_SEARCH_URL = os.environ['OPEN_SEARCH_URL']
    OPEN_SEARCH_INDEX = os.environ['OPEN_SEARCH_INDEX']
    OPEN_SEARCH_USERNAME = os.environ['OPEN_SEARCH_USERNAME']
    OPEN_SEARCH_PASSWORD = os.environ['OPEN_SEARCH_PASSWORD']

    search_query = f'{OPEN_SEARCH_URL}/{OPEN_SEARCH_INDEX}/_search?q={cuisine}'

    search_response = requests.get(search_query, auth = (OPEN_SEARCH_USERNAME, OPEN_SEARCH_PASSWORD))
    
    logger.info(f'Open search request = {search_query} and response = {search_response}')

    open_search_data = {}
    open_search_data = json.loads(search_response.content.decode('utf-8'))['hits']['hits']
    
    logger.info(f'Open search data = {open_search_data}')
    
    for data in open_search_data:
        businessIds.append(data['_id'])
      
    return businessIds


def lambda_handler(event, context):
    
    #Step 1: Get messages from SQS
    sqs = boto3.client('sqs')
    sqs_url = sqs.get_queue_url(QueueName='DiningQueue')['QueueUrl']
    
    sqs_response = sqs.receive_message(QueueUrl=sqs_url, MaxNumberOfMessages=1)
    logger.info(f'Response from sqs is {sqs_response}')
    messages = []
    if 'Messages' in sqs_response:
        messages = sqs_response['Messages']

    for message in messages:
        print('here')
        # Step 2: Get restaurant recommendation from elastic search and dynamo db
        logger.info(f'Serving the following message: {message}')
        
        # a) Get data from message
        body = message["Body"].replace("\'", "\"")
        details = json.loads(body)

        cuisine = details['Cuisine']
        num_people = details['NumberOfPeople']
        #ph_no = details['PhoneNumber']
        email = details['Email']
        dining_time = details['Time']
        dining_date = details['Date']
        location = details['Location']
        
        # b) Get BusinessId from elastic search
        businessIds = get_data_from_elastic_search(cuisine)
        
        # c) Get other details from dynamo
        dynamo_data = get_dynamo_data(businessIds)
        
        #Step 3: Format message and send to user
        
        # a) Build message to send to user
        text_message = build_message(dynamo_data, cuisine, num_people, location, dining_date, dining_time)
        
        send_email(text_message, email)

        
        # c) Finally, delete message from SQS
        receipt_handle = message['ReceiptHandle']
        sqs_response = sqs.delete_message(QueueUrl = sqs_url, ReceiptHandle = receipt_handle)
        logger.info(f'Response from sqs for deleting {receipt_handle} is {sqs_response}')
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
