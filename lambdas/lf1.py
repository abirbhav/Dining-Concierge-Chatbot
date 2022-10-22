import math
import dateutil.parser
import datetime
import time
import os
import logging
import re

import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
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


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


# """ --- Helper Functions --- """


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False

def push_to_queue(slots):
    logger.debug('Sending to sqs')
    sqs = boto3.client('sqs')
    sqs_url = sqs.get_queue_url(QueueName='DiningQueue')['QueueUrl']
    logger.info(f'slots converted to string is {str(slots)}')
    response = sqs.send_message(
        QueueUrl = sqs_url,
        MessageBody = str(slots)
        )
    logger.info(f'Response from message queue = {response}')

def validate_dining_parameters(location, cuisine, dining_date, dining_time, num_people, email):
    locations = ['manhattan']
    cuisines = ['chinese', 'indian', 'italian', 'mexican', 'thai']
    if location is not None and location.lower() not in locations:
        return build_validation_result(False,
                                      'Location',
                                      'We are currently only serving Manhattan.')
    if cuisine is not None and cuisine.lower() not in cuisines:
        return build_validation_result(False,
                                      'Cuisine',
                                      f"We don't offer suggestions for {cuisine} yet. Please try another cuisine.")
    if num_people is not None:
        num_people = int(num_people)
        if num_people <= 0 or num_people > 10:
            return build_validation_result(False,
                                      'NumberOfPeople',
                                      f"The number of people should be between 0 and 10 (inclusive)")
    if dining_date is not None:
        if not isvalid_date(dining_date):
            return build_validation_result(False, 'Date', 'Please provide a valid date')
        elif datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() <= datetime.date.today():
            return build_validation_result(False, 'Date', 'Date must be >= tomorrow')
            
    if dining_time is not None:
        if len(dining_time) != 5:
            return build_validation_result(False, 'Time', 'Please provide a valid time')

        hour, minute = dining_time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            return build_validation_result(False, 'Time', 'Please provide a valid time')
      
    # if ph_no is not None:
    #     if len(ph_no) != 10 or not ph_no.isnumeric():
    #         return build_validation_result(False, 'PhoneNumber', 'Please provide a valid 10 digit Phone numer without special characters')
    
    email_regex = re.compile(r"[^@]+@[^@]+\.[^@]+")

    if email is not None:
        if not email_regex.match(email):
            return build_validation_result(False, 'Time', 'Please provide a valid email')

    return build_validation_result(True, None, None)


def dining_suggestions(intent_request):
    
    slots = get_slots(intent_request)
    location = slots["Location"]
    cuisine = slots["Cuisine"]
    dining_date = slots["Date"]
    dining_time = slots["Time"]
    num_people = slots["NumberOfPeople"]
    #ph_no = slots["PhoneNumber"]
    email = slots["Email"]
    
    source = intent_request['invocationSource']
    
    
    if source == 'DialogCodeHook':
        # Validate
        # Use the elicitSlot dialog action to re-prompt for the first violation detected.

        validation_result = validate_dining_parameters(location, cuisine, dining_date, dining_time, num_people, email)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                              intent_request['currentIntent']['name'],
                              slots,
                              validation_result['violatedSlot'],
                              validation_result['message'])
                              
        # This is the case when it is valid
        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}

        return delegate(output_session_attributes, slots)

    #Push to queue
    push_to_queue(slots)
    
    # Final message from bot
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'I will send my suggestions to the email address you provided shortly'})

def greet(intent_request):
    bot_message = 'Hi there, how can I help?'
    return {
        "dialogAction": {
        "type": "ElicitIntent",
        "message": {
        "contentType": "PlainText",
        "content": bot_message
        }
        }
    }

def thank_you(intent_request):
    bot_message = 'No problem. Thank you for using the dining concierge bot.'
    return {
        "dialogAction": {
        "type": "Close",
        "fulfillmentState": "Fulfilled",
        "message": {
            "contentType": "PlainText",
            "content": bot_message
        }
    }
    }


""" --- Intents --- """


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'GreetingIntent':
        return greet(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thank_you(intent_request)
    

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    logger.info(f'event is {event}')
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))
    resp = dispatch(event)
    logger.info(f'response if {resp}')

    return resp
