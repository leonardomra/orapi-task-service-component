from orcomm_module.orevent import OREvent 
from flask import jsonify
import json
import boto3

class ORTopic():

    def __init__(self, AWS_REGION=None, AWS_ACCESS_KEY=None, AWS_SECRET_KEY=None, TOPIC_ARN=None):
        self.sns = boto3.client(
            'sns',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        self.TopicArn = TOPIC_ARN

    def broadcastEvent(self, response):
        self.sns.publish(
            TopicArn=self.TopicArn,
            Subject=response.Subject,
            Message=json.dumps({ "default": response.Message }),
            MessageStructure=response.MessageStructure
        )
        return jsonify(response.summarize())

    def tuneTopic(self, headers, body):        
        e = OREvent()
        if 'x-amz-sns-message-type' in headers:
            e.Type = headers['x-amz-sns-message-type']
        if 'x-amz-sns-message-id' in headers:
            e.MessageId = headers['x-amz-sns-message-id']
        if 'x-amz-sns-topic-arn' in headers:
            e.TopicArn = headers['x-amz-sns-topic-arn']
        if 'x-amz-sns-subscription-arn' in headers:
            e.SubscriptionArn = headers['x-amz-sns-subscription-arn']
        if 'Timestamp' in body:
            e.Timestamp = body['Timestamp']
        if 'SignatureVersion' in body:
            e.SignatureVersion = body['SignatureVersion']
        if 'Signature' in body:
            e.Signature = body['Signature']
        if 'SigningCertURL' in body:
            e.SigningCertURL = body['SigningCertURL']
        if 'UnsubscribeURL' in body:
            e.UnsubscribeURL = body['UnsubscribeURL']  
        if 'Token' in body:
            e.Token = body['Token']
        if 'Subject' in body:
            e.Subject = body['Subject']
        if 'Message' in body:
            e.Message = body['Message']
        return e

    def confirmSubscription(self, response):
        return self.sns.confirm_subscription(
            TopicArn = self.TopicArn,
            Token = response.Token,
        )

    def unsubscribe(self, response):
        return self.sns.unsubscribe(
            SubscriptionArn = response.SubscriptionArn,
        )

    def getResource(self):
        return self.sns