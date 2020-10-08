import os
import io
import gc
import json
import time
import uuid
import shutil
import sys
import zipfile
import pandas as pd
from pathlib import Path
from aihandler.tml.topicdiscoverer import TopicDiscoverer
from aihandler.tml.topicmodeller import TopicModeller
from threading import Timer
from task_module.models.job import Job
from task_module.models.datacomplex import DataComplex
from orcomm_module.orevent import OREvent


class TSK:


    def __init__(self, db, s3, orcomm):
        self.db = db
        self.s3 = s3
        self.orcomm = orcomm
        self.timer = None
        self.intervalIsActive = False
        self.taskIsActive = False
        self._taskKind = None


    def startTimer(self):
        print('>>>', 'INFO:', 'start listening for tasks:', self._taskKind, '...', flush=True)
        while True:
            gc.collect()
            time.sleep(2)
            self.getTask()


    def getTask(self):  # noqa: E501
        #print('INFO:', 'listening:', self._taskKind, '...', flush=True)
        if self.taskIsActive == True:
            print('INFO:', 'will execute task:', self._taskKind, '...', flush=True)
        else:
            if os.environ['TASK'] == 'train-' + self._taskKind:
                if self._taskKind == 'tml':
                    items = self.orcomm.itemsForQueue(os.environ['TRAIN_SQS_QUEUE_NAME_TML'], os.environ['TRAIN_SQS_QUEUE_ARN_TML'], ['jobId', 'jobStatus', 'jobTask', 'jobKind', 'order'], 1, False)
                elif self._taskKind == 'qna':  
                    items = self.orcomm.itemsForQueue(os.environ['TRAIN_SQS_QUEUE_NAME_QNA'], os.environ['TRAIN_SQS_QUEUE_ARN_QNA'], ['jobId', 'jobStatus', 'jobTask', 'jobKind', 'order'], 1, False)
                #items = self.orcomm.itemsForQueue(os.environ['TRAIN_SQS_QUEUE_NAME'], os.environ['TRAIN_SQS_QUEUE_ARN'], ['jobId', 'jobStatus', 'jobTask', 'jobKind', 'order'], 1, False)
            elif os.environ['TASK'] == 'analyse-' + self._taskKind:
                if self._taskKind == 'tml':
                    items = self.orcomm.itemsForQueue(os.environ['PREDICT_SQS_QUEUE_NAME_TML'], os.environ['PREDICT_SQS_QUEUE_ARN_TML'], ['jobId', 'jobStatus', 'jobTask', 'jobKind', 'order'], 1, False)
                elif self._taskKind == 'qna':
                    items = self.orcomm.itemsForQueue(os.environ['PREDICT_SQS_QUEUE_NAME_QNA'], os.environ['PREDICT_SQS_QUEUE_ARN_QNA'], ['jobId', 'jobStatus', 'jobTask', 'jobKind', 'order'], 1, False)
                #items = self.orcomm.itemsForQueue(os.environ['PREDICT_SQS_QUEUE_NAME'], os.environ['PREDICT_SQS_QUEUE_ARN'], ['jobId', 'jobStatus', 'jobTask', 'jobKind', 'order'], 1, False)  
            if not items:
                #print('Waiting for', os.environ['TASK'], 'task...', flush=True)
                pass
            else:
                item = items[0]
                if item.MessageAttributes['jobKind']['StringValue'] != self._taskKind:
                    return None
                self.taskIsActive = True
                task = self.postTask(job=item.MessageAttributes['jobId']['StringValue'], queueItem=item)
                if task['status']:
                    print('INFO:', 'task succeeded!', flush=True)
                else:
                    print('INFO:', 'task failed.', flush=True)
                try:
                    print(self.sendEventForFinishedJob(task), flush=True) # SEND MESSAGE AS WELL - do not send message to the user if the task was already executed, please!
                except Exception as e:
                    print('ERROR getTask0:', e, flush=True)
                try:
                    if os.environ['TASK'] == 'train-' + self._taskKind:
                        if self._taskKind == 'tml':
                            self.orcomm.getQueue(os.environ['TRAIN_SQS_QUEUE_ARN_TML']).deleteItem(item.QueueUrl, item.ReceiptHandle)
                        elif self._taskKind == 'qna':
                            self.orcomm.getQueue(os.environ['TRAIN_SQS_QUEUE_ARN_QNA']).deleteItem(item.QueueUrl, item.ReceiptHandle)
                    elif os.environ['TASK'] == 'analyse-' + self._taskKind:
                        if self._taskKind == 'tml':
                            self.orcomm.getQueue(os.environ['PREDICT_SQS_QUEUE_ARN_TML']).deleteItem(item.QueueUrl, item.ReceiptHandle)
                        elif self._taskKind == 'qna':
                            self.orcomm.getQueue(os.environ['PREDICT_SQS_QUEUE_ARN_QNA']).deleteItem(item.QueueUrl, item.ReceiptHandle)
                    #self.orcomm.getQueue(os.environ['PREDICT_SQS_QUEUE_ARN']).deleteItem(item.QueueUrl, item.ReceiptHandle)
                except Exception as e:
                    print('ERROR getTask1:', e, flush=True)


    def postTask(self, job=None, queueItem=None):  # noqa: E501
        jobQuery = ("SELECT id, label, description, kind, status, model, dataSource, dataSample, output, task, taskParams, user FROM Job WHERE id = %s")
        paramsJob = (job,)
        resultsJob = self.db.get(jobQuery, paramsJob)
        if not resultsJob:
            self.cancellation(job, 'Job is invalid.')
            return {'status': False, 'job': None, 'message': 'Job is invalid.' }
        job = Job()
        job.id = resultsJob[0][0]
        job.label = resultsJob[0][1]
        job.description = resultsJob[0][2]
        job.kind = resultsJob[0][3]
        job.status = resultsJob[0][4]
        job.model = resultsJob[0][5]
        job.data_source = resultsJob[0][6]
        job.data_sample = resultsJob[0][7]
        job.output = resultsJob[0][8]
        job.task = resultsJob[0][9]
        job.task_params = resultsJob[0][10]
        job.task_params = json.loads(job.task_params)
        job.user = resultsJob[0][11]
        # populate source, sample and model
        print(job.id, job.status, flush=True)
        if job.status == 'completed' or job.status == 'cancelled':
            print('Task already executed.', flush=True)
            self.taskIsActive = False
            return {'status': False, 'job': job, 'message': 'Task already executed.', 'code': 'executed' }
        dataQuery = ("SELECT id, fileName, format, kind, label, location FROM Data WHERE id = %s")
        if job.task == 'train':
            dataSource = self.populateDataComplex(job, job.data_source, dataQuery, 'Data source is invalid.')
            if not dataSource:
                self.taskIsActive = False
                return {'status': False, 'job': job, 'message': 'Data source is invalid.', 'code': 'error' }
            else:
                job.data_source = dataSource
        else:
            dataSample = self.populateDataComplex(job, job.data_sample, dataQuery, 'Data sample is invalid.')
            if not dataSample and job.kind == 'tml':
                self.taskIsActive = False
                return {'status': False, 'job': job, 'message': 'Data sample is invalid.' }
            else:
                job.data_sample = dataSample
            model = self.populateDataComplex(job, job.model, dataQuery, 'Model is invalid.')
            if not model and job.kind == 'tml':
                self.taskIsActive = False
                return {'status': False, 'job': job, 'message': 'Model is invalid.' }
            else:
                job.model = model
        if job.kind == self._taskKind:
            response = self.execML(job)
            self.taskIsActive = False
            return {'status': response['status'], 'job': job, 'message': response['msg'], 'code': response['code'] }
        else:
            return {'status': False, 'job': job, 'message': '', 'code': 'unfitting' }
    

    def execML(self, job): 
        #return True
        return {'status': False, 'code': 'interface', 'msg': None }


    def sendEventForFinishedJob(self, task):
        # create event
        job = task['job']
        jobDict = job.__dict__.copy()
        del jobDict['swagger_types']
        del jobDict['attribute_map']
        event = OREvent()
        event.TopicArn = os.environ['JOBS_ARN_TOPIC']
        event.Subject = 'Finish Job'
        jjson = json.loads(json.dumps(jobDict))
        jjson['code'] =  task['code']
        jjson['message'] =  task['message']
        print(jjson, flush=True)
        event.Message = json.dumps(jjson)
        response = self.orcomm.getTopic(os.environ['JOBS_ARN_TOPIC']).broadcastEvent(event)
        return response


    def populateDataComplex(self, job, target, dataQuery, errorMsg):
        paramsData = (target,)
        resultsData = self.db.get(dataQuery, paramsData)
        if not resultsData:
            self.cancellation(job, errorMsg)
            return False
        target = {
            'id': resultsData[0][0],
            'fileName': resultsData[0][1], 
            'format': resultsData[0][2],
            'kind': resultsData[0][3],
            'label': resultsData[0][4],
            'location': resultsData[0][5]
        }
        return target


    def persistResult(self, job, result):
        inMemoryFile = io.BytesIO()
        inMemoryFile.write(json.dumps(result).encode())
        inMemoryFile.seek(0) 
        if job.kind == 'tml':   
            locationSplit = job.model['location'].split('/')
        elif job.kind == 'qna':
            locationSplit = ['openresearch', job.user]
        bucketName = locationSplit[0]
        user = locationSplit[1]
        s3Resp = self.s3.uploadFileObject(inMemoryFile, bucketName, user + '/' + job.id + '_' + self._taskKind + '-result.json')
        if s3Resp:
            dataset = DataComplex()
            dataset.id = str(uuid.uuid4())
            dataset.file_name = job.id + '_' + self._taskKind + '-result.json'
            dataset.location = bucketName + '/' + user + '/' + job.id + '_' + self._taskKind + '-result.json'
            dataset.kind = 'result'
            dataset.format = 'application/json'
            dataset.label = 'Results of job ' + job.id + '.'  
            # store persistent data
            add_dataset = ("INSERT INTO Data "
                    "(id, fileName, format, kind, label, location) "
                    "VALUES (%s, %s, %s, %s, %s, %s)")
            data_dataset = (dataset.id, dataset.file_name, dataset.format, dataset.kind, dataset.label, dataset.location)
            self.db.add(add_dataset, data_dataset)
            updateJobQuery = ("UPDATE Job SET output = %s WHERE id = %s")
            paramsStart = (dataset.id, job.id)
            self.db.update(updateJobQuery, paramsStart)


    def downloadAndConvertCSV(self, job, target):
        csvData = None
        locationSplit = target['location'].split('/')
        bucketName = locationSplit[0]
        key = locationSplit[1] + '/' + locationSplit[2]
        fileData = self.s3.downloadFile(bucketName, key)
        if not fileData:
            return self.cancellation(job, 'Problem downloading object from S3.')
        try:
            csvData = pd.read_csv(fileData, sep = ',')
        except Exception as e:
            return self.cancellation(job, e)
        return csvData
    

    def downloadAndStoreTMLModel(self, job, target):
        locationSplit = target['location'].split('/')
        bucketName = locationSplit[0]
        key = locationSplit[1] + '/' + locationSplit[2]
        fileData = self.s3.downloadFile(bucketName, key)
        if not fileData:
            return self.cancellation(job, 'Problem downloading object from S3.')
        fileData.seek(0)
        with open('tmp/' + self._taskKind + '-model.tml', 'wb') as f:
            shutil.copyfileobj(fileData, f)
        del fileData
        return True


    def downloadAndStoreDataset(self, job, target):
        locationSplit = target['location'].split('/')
        bucketName = locationSplit[0]
        key = locationSplit[1] + '/' + locationSplit[2]
        fileData = self.s3.downloadFile(bucketName, key)
        if not fileData:
            return self.cancellation(job, 'Problem downloading object from S3.')
        fileData.seek(0)
        Path('tmp/' + target['id']).mkdir(parents=True, exist_ok=True)
        with open('tmp/' + target['id'] + '/' + target['fileName'], 'wb') as f:
            shutil.copyfileobj(fileData, f)
        del fileData
        return True


    def downloadAndStoreZIPModel(self, job, target):
        modelIsPresent = False
        mappings = json.loads(os.environ['DEFAULT_MODEL_MAPPINGS'])
        for m in mappings:
            for key in list(m.keys()):
                for d in os.scandir('./tmp'):
                    if m[key] == d.name:
                        modelIsPresent = True
        if modelIsPresent:
            return True
        locationSplit = target['location'].split('/')
        bucketName = locationSplit[0]
        key = locationSplit[1] + '/' + locationSplit[2]
        fileData = self.s3.downloadFile(bucketName, key)
        if not fileData:
            return self.cancellation(job, 'Problem downloading object from S3.')
        fileData.seek(0)
        Path('tmp/' + job.model['id']).mkdir(parents=True, exist_ok=True)
        zf = zipfile.ZipFile(fileData, 'r')
        for name in zf.namelist():
            with open('tmp/' + job.model['id'] + '/' + name, 'wb') as f:
                f.write(zf.read(name))
        del fileData
        del zf
        return True


    def updateJobStatus(self, job, status):
        updateJobQuery = ("UPDATE Job SET status = %s WHERE id = %s")
        paramsStart = (status, job.id)
        self.db.update(updateJobQuery, paramsStart)


    def cancellation(self, job, message):
        print('ERROR:', message , flush=True)
        self.updateJobStatus(job, 'cancelled')
        self.taskIsActive = False
        return False


    def persistTMLModel(self, vectorsBin, job):
        locationSplit = job.data_source['location'].split('/')
        bucketName = locationSplit[0]
        user = locationSplit[1]
        s3Resp = self.s3.uploadFileObject(vectorsBin, bucketName, user + '/' + job.id + '_' + self._taskKind + '-model.tml')
        if s3Resp:
            dataset = DataComplex()
            dataset.id = str(uuid.uuid4())
            dataset.file_name = job.id + '_' + self._taskKind + '-model.tml'
            dataset.location = bucketName + '/' + user + '/' + job.id + '_' + self._taskKind + '-model.tml'
            dataset.kind = 'model'
            dataset.format = 'application/octet-stream'
            dataset.label = 'Model created for user ' + user + ' as a result of a TML Training job.'  
            # store persistent data
            add_dataset = ("INSERT INTO Data "
                    "(id, fileName, format, kind, label, location) "
                    "VALUES (%s, %s, %s, %s, %s, %s)")
            data_dataset = (dataset.id, dataset.file_name, dataset.format, dataset.kind, dataset.label, dataset.location)
            self.db.add(add_dataset, data_dataset)
            updateJobQuery = ("UPDATE Job SET output = %s WHERE id = %s")
            paramsStart = (dataset.id, job.id)
            self.db.update(updateJobQuery, paramsStart)
        del vectorsBin


    def persistQNAModel(self, newModelId, job):
        path = 'tmp/' + newModelId
        inMemoryFile = io.BytesIO()
        zf = zipfile.ZipFile(inMemoryFile, 'w', zipfile.ZIP_DEFLATED)
        for root, dirs, files in os.walk(path):
            for _file in files:
                zf.write(os.path.join(root, _file), _file)
        zf.close()
        inMemoryFile.seek(0)
        locationSplit = job.data_source['location'].split('/')
        bucketName = locationSplit[0]
        user = locationSplit[1]
        s3FileName = job.id + '_' + newModelId + '_' + self._taskKind + '-model.zip'
        s3Resp = self.s3.uploadFileObject(inMemoryFile.getvalue(), bucketName, user + '/' + s3FileName)
        if s3Resp:
            dataset = DataComplex()
            dataset.id = newModelId
            dataset.file_name = s3FileName
            dataset.location = bucketName + '/' + user + '/' + s3FileName
            dataset.kind = 'model'
            dataset.format = 'application/zip'
            dataset.label = 'Model created for user ' + user + ' as a result of a QNA Training job.'  
            # store persistent data
            add_dataset = ("INSERT INTO Data "
                    "(id, fileName, format, kind, label, location) "
                    "VALUES (%s, %s, %s, %s, %s, %s)")
            data_dataset = (dataset.id, dataset.file_name, dataset.format, dataset.kind, dataset.label, dataset.location)
            self.db.add(add_dataset, data_dataset)
            updateJobQuery = ("UPDATE Job SET output = %s WHERE id = %s")
            paramsStart = (dataset.id, job.id)
            self.db.update(updateJobQuery, paramsStart)
        del inMemoryFile