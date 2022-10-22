import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

num_messages_to_get = 5


def build_message(dynamo_data):
    message = ['Your suggestions are as follows: \n']
    for restaurant_data in dynamo_data:
        pass
    return ''.join(message)

def get_dynamo_data(businessIds):
    data = []
    dynamo_db = boto3.resource('dynamodb')
    table = client.Table('yelp-restaurants')
    for businessId in businessIds:
        dynamo_response = table.get_item(Key={'Business ID': businessId})
        data.append(dynamo_response)
    return data

def get_data_from_elastic_search(cuisine):
    businessIds = []
    elastic_url = ''
    elastic_index = ''
    elastic_username = ''
    elastic_password = ''
    
    search_query = f'{elastic_url}{elastic_index}/_search?q={cuisine}'
    #search_response = 
    
    logger.info(f'Request to elastic search: {search_query}, Response from elastic search: {search_response}')
    return businessIds


def lambda_handler(event, context):
    
    #Step 1: Get messages from SQS
    sqs = boto3.client('sqs')
    sqs_url = sqs.get_queue_url(QueueName='DiningQueue')['QueueUrl']
    
    sqs_response = sqs.receive_message(QueueUrl=sqs_url, MaxNumberOfMessages=1)
    logger.info(f'Response from sqs is {sqs_response}')
    messages = []
    if 'messages' in sqs_response:
        messages = response_sqs['messages']
    
    for message in messages:
        # Step 2: Get restaurant recommendation from elastic search and dynamo db
        logger.info(f'Serving the following message: {message}')
        
        # a) Get data from message
        cuisine = message['Body']['Cuisine']
        num_people = message['Body']['NumberOfPeople']
        ph_no = message['Body']['PhoneNumber']
        dining_time = message['Body']['Time']
        dining_date = message['Body']['Date']
        location = message['Body']['Location']
        
        # b) Get BusinessId from elastic search
        businessIds = get_data_from_elastic_search(cuisine)
        
        # c) Get other details from dynamo
        dynamo_data = get_dynamo_data(businessIds)
        
        #Step 3: Format message and send to user
        
        # a) Build message to send to user
        test_message = build_message(dynamo_data)
        
        # b) Send the Text Message
        print(text_message)
        # sns = boto3.client('sns')
        # sns_response = sns.publish(PhoneNumber = f'+1{ph_no}', Message=msgToSend)
        # logger.info(f'SNS response is {sns_response}')
        
        # c) Finally, delete message from SQS
        receipt_handle = message['ReceiptHandle']
        sqs_response = sqs.delete_message(QueueUrl = sqs_url, ReceiptHandle = receipt_handle)
        logger.info(f'Response from sqs for deleting {receipt_handle} is {sqs_response}')
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
