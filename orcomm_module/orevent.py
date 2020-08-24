class OREvent():

    def __init__(self):
        self._type = None
        self._messageId = None
        self._timestamp = None
        self._signatureVersion = None
        self._signature = None
        self._signingCertURL = None
        self._unsubscribeURL = None
        self._topicArn = None
        self._token = None
        self._subject = None
        self._message = None
        self._subscriptionArn = None
        self._messageStructure = 'json'

    @property
    def Type(self) -> str:
        return self._type

    @Type.setter
    def Type(self, _type: str):
        self._type = _type

    @property
    def MessageId(self) -> str:
        return self._messageId

    @MessageId.setter
    def MessageId(self, _messageId: str):
        self._messageId = _messageId

    @property
    def Timestamp(self) -> str:
        return self._timestamp

    @Timestamp.setter
    def Timestamp(self, _timestamp: str):
        self._timestamp = _timestamp

    @property
    def SignatureVersion(self) -> str:
        return self._signatureVersion

    @SignatureVersion.setter
    def SignatureVersion(self, _signatureVersion: str):
        self._signatureVersion = _signatureVersion

    @property
    def Signature(self) -> str:
        return self._signature

    @Signature.setter
    def Signature(self, _signature: str):
        self._signature = _signature

    @property
    def SigningCertURL(self) -> str:
        return self._signingCertURL

    @SigningCertURL.setter
    def SigningCertURL(self, _signingCertURL: str):
        self._signingCertURL = _signingCertURL

    @property
    def UnsubscribeURL(self) -> str:
        return self._unsubscribeURL

    @UnsubscribeURL.setter
    def UnsubscribeURL(self, _unsubscribeURL: str):
        self._unsubscribeURL = _unsubscribeURL

    @property
    def TopicArn(self) -> str:
        return self._topicArn

    @TopicArn.setter
    def TopicArn(self, _topicArn: str):
        self._topicArn = _topicArn

    @property
    def Token(self) -> str:
        return self._token

    @Token.setter
    def Token(self, _token: str):
        self._token = _token

    @property
    def Subject(self) -> str:
        return self._subject

    @Subject.setter
    def Subject(self, _subject: str):
        self._subject = _subject

    @property
    def Message(self) -> {}:
        return self._message

    @Message.setter
    def Message(self, _message: {}):
        self._message = _message

    @property
    def MessageStructure(self) -> str:
        return self._messageStructure

    @MessageStructure.setter
    def MessageStructure(self, _messageStructure: str):
        self._messageStructure = _messageStructure

    @property
    def SubscriptionArn(self) -> str:
        return self._subscriptionArn

    @SubscriptionArn.setter
    def SubscriptionArn(self, _subscriptionArn: str):
        self._subscriptionArn = _subscriptionArn

    def summarize(self):
        return {
            'Type': self._type,
            'MessageId': self._messageId,
            'Timestamp': self._timestamp,
            'SignatureVersion': self._signatureVersion,
            'SignatureVersion': self._signatureVersion,
            'SigningCertURL': self._signingCertURL,
            'UnsubscribeURL': self._unsubscribeURL,
            'TopicArn': self._topicArn,
            'Token': self._token,
            'Subject': self._subject,
            'Message': self._message,
            'SubscriptionArn': self._subscriptionArn,
            'MessageStructure': self._messageStructure,
        }