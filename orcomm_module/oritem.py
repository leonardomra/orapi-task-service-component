class ORItem():

    def __init__(self):
        self._attributes = {}
        self._body = None
        self._messageAttributes = {}
        self._messageId = None
        self._messageIsActive = False
        self._receipt_handle = None
        self._queue_url = None

    @property
    def Attributes(self) -> {}:
        return self._attributes

    @Attributes.setter
    def Attributes(self, _attributes: {}):
        """
        Supported attributes:
            ApproximateReceiveCount
            ApproximateFirstReceiveTimestamp
            MessageDeduplicationId
            MessageGroupId
            SenderId
            SentTimestamp
            SequenceNumber
        """
        self._attributes = _attributes

    @property
    def MessageAttributes(self) -> {}:
        return self._messageAttributes

    @MessageAttributes.setter
    def MessageAttributes(self, _messageAttributes: {}):
        """
        Example:
            {
                'Author': {
                    'StringValue': 'Daniel',
                    'DataType': 'String'
                }
            }
        """
        self._messageAttributes = _messageAttributes

    @property
    def MessageBody(self) -> str:
        return self._body

    @MessageBody.setter
    def MessageBody(self, _body: str):
        self._body = _body

    @property
    def MessageId(self) -> str:
        return self._messageId

    @MessageId.setter
    def MessageId(self, _messageId: str):
        self._messageId = _messageId

    @property
    def MessageIsActive(self) -> bool:
        return self._messageIsActive

    @MessageIsActive.setter
    def MessageIsActive(self, _messageIsActive: bool):
        self._messageIsActive = _messageIsActive

    @property
    def ReceiptHandle(self) -> bool:
        return self._receipt_handle

    @ReceiptHandle.setter
    def ReceiptHandle(self, _receipt_handle: bool):
        self._receipt_handle = _receipt_handle

    @property
    def QueueUrl(self) -> bool:
        return self._queue_url

    @QueueUrl.setter
    def QueueUrl(self, _queue_url: bool):
        self._queue_url = _queue_url

    def summarize(self):
        return {
            'Attributes': self._attributes,
            'MessageAttributes': self._messageAttributes,
            'MessageBody': self._body,
            'MessageId': self._messageId,
            'MessageIsActive': self._messageIsActive,
            'ReceiptHandle': self._receipt_handle,
            'QueueUrl': self._queue_url,
        }