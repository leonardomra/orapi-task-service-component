import boto3
import json
import time
from orcomm_module.oritem import ORItem

class ORQueue():

    def __init__(self, AWS_REGION=None, AWS_ACCESS_KEY=None, AWS_SECRET_KEY=None, QUEUE_NAME=None, QUEUE_URL=None):
        self.queueURL = QUEUE_URL
        session  = boto3.Session(
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        self.sqs = session.resource('sqs')
        self.queue = self.sqs.get_queue_by_name(QueueName=QUEUE_NAME)
        self.sqsClient = boto3.client(
            'sqs',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )

    def pushItem(self, oritem):
        return self.queue.send_message(MessageBody=oritem.MessageBody, MessageAttributes=oritem.MessageAttributes, MessageGroupId=str(int(time.time())))
        
    def pullItems(self, messageAttributeNames=[], limit=1, deleteMsgs=False):
        messages = self.queue.receive_messages(MessageAttributeNames=messageAttributeNames, MaxNumberOfMessages=limit)
        #response = self.sqsClient.receive_message(QueueUrl=self.queueURL,MessageAttributeNames=messageAttributeNames, MaxNumberOfMessages=limit)
        items = []
        '''
        messages = []
        if 'Messages' in response:
            messages = response['Messages']
        '''
        #print('..' , messages, flush=True)
        
        for message in messages:
            item = ORItem()
            
            '''
            item.MessageAttributes = message['message_attributes']
            item.Attributes = message['attributes']
            item.MessageBody = message['body']
            item.QueueUrl = message['queue_url']
            item.MessageId = message['message_id']
            item.ReceiptHandle = message['receipt_handle']
            item.MessageIsActive = not deleteMsgs
            '''

            item.MessageAttributes = message.message_attributes
            item.Attributes = message.attributes
            item.MessageBody = message.body
            item.QueueUrl = message.queue_url
            item.MessageId = message.message_id
            item.ReceiptHandle = message.receipt_handle
            item.MessageIsActive = not deleteMsgs
            if deleteMsgs:
                message.delete()
            items.append(item)
            
        return items

    def getResource(self):
        return self.queue

    def deleteItem(self, queueUrl, receiptHandle):
        return self.sqsClient.delete_message(
            QueueUrl=queueUrl,
            ReceiptHandle=receiptHandle
        )