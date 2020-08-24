from orcomm_module.ortopic import ORTopic 
from orcomm_module.orchannel import ORChannel
from orcomm_module.orqueue import ORQueue

import boto3

class ORCommunicator():
    
    def __init__(self, AWS_REGION=None, AWS_ACCESS_KEY=None, AWS_SECRET_KEY=None):
        self.awsRegion = AWS_REGION
        self.awsAccessKey = AWS_ACCESS_KEY
        self.awsSecretKey = AWS_SECRET_KEY
        self.topics = {}
        self.channels = {}
        self.queues = {}
        
    def addTopic(self, topicName=None, topicArn=None):
        topic = ORTopic(self.awsRegion, self.awsAccessKey, self.awsSecretKey, topicArn)
        self.topics[topicArn] = topic

    def addChannel(self, channelName):
        channel = ORChannel(channelName)
        self.channels[channelName] = channel

    def addQueue(self, queueName=None, queueArn=None):
        queue = ORQueue(self.awsRegion, self.awsAccessKey, self.awsSecretKey, queueName)
        self.queues[queueArn] = queue

    def getTopics(self):
        return self.topics

    def getChannels(self):
        return self.channels

    def getQueues(self):
        return self.queues

    def itemsForQueue(self, queueName, queueArn, messageAttributeNames=[], limit=1, deleteMsgs=False):
        if queueArn not in self.queues:
            self.queues[queueArn] = ORQueue(self.awsRegion, self.awsAccessKey, self.awsSecretKey, queueName)
        return self.queues[queueArn].pullItems(messageAttributeNames, limit, deleteMsgs)

    def getTopic(self, topicArn):
        return self.topics[topicArn]

    def getChannel(self, channelName):
        return self.channels[channelName]

    def getQueue(self, queueArn):
        return self.queues[queueArn]

    