from aihandler.tml.lda import LDA

class TopicModeller():

    def __init__(self, s3):
        self.shouldUseDump = False

    def setDumpUsage(self, shouldUseDump):
        self.shouldUseDump = shouldUseDump

    def CSV2Vectors(self, dataset, taskParams):
        try:
            self.target = self.selectTargetColumn(dataset, taskParams['selectTargetColumn'])
        except Exception as e:
            return False
        lda = LDA()
        return lda.vectorize(self.target)

    def CSV2Topics(self, sample, taskParams):
        target = None
        try:
            target = self.selectTargetColumn(sample, taskParams['selectTargetColumn'])
        except Exception:
            return False
        lda = LDA()
        return lda.produceTopics(target, taskParams)
    
    def CSV2TermAmount(self, data, taskParams):
        lda = LDA()
        self.data = data
        self.target = self.selectTargetColumn(data, 'abstract')
        return lda.countWords(self.target, self.shouldUseDump)

    def selectTargetColumn(self, data, columnName):
        return data[columnName]
