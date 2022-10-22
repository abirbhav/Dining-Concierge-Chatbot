import json
import boto3
import logging
import os

  
BOT_NAME = os.environ['BOT_NAME']
BOT_ALIAS = os.environ['BOT_ALIAS']
USER_ID = os.environ['USER_ID']


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def lambda_handler(event, context):
    
    logger.info(f'event is {event}')
    
    lex = boto3.client('lex-runtime')
    
    user_message = event["messages"][0]["unstructured"]["text"]
    # sending request to lex
    response = lex.post_text(
        botName = BOT_NAME,
        botAlias = BOT_ALIAS,
        userId = USER_ID,
        inputText = user_message)
    logger.info(f'response from lex is {response}')
    
    bot_message = response['message']
    
    return {
        "messages":[{"type":"unstructured","unstructured":{"text":bot_message}}]}
