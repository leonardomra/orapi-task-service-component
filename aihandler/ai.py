import os
import io
import gc
import json
import time
import uuid
import shutil
import sys
import pandas as pd
from aihandler.tml.topicdiscoverer import TopicDiscoverer
from aihandler.tml.topicmodeller import TopicModeller
from dbhandler.mysql_handler import MySQLHandler
from s3handler.s3_handler import S3Handler
from orcomm_module.orcommunicator import ORCommunicator
from threading import Timer
from task_module.models.job import Job
from task_module.models.datacomplex import DataComplex
from orcomm_module.orevent import OREvent 

#sys.setrecursionlimit(100)

class AIntel:

    def __init__(self):
        self.db = MySQLHandler(os.environ['MYSQL_USER'], os.environ['MYSQL_PASSWORD'], os.environ['MYSQL_HOST'], os.environ['MYSQL_DATABASE'])
        self.s3 = S3Handler(os.environ['AWS_REGION'], os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'])
        self.orcomm = ORCommunicator(os.environ['AWS_REGION'], os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'])
        self.orcomm.addQueue(os.environ['TRAIN_SQS_QUEUE_NAME'], os.environ['TRAIN_SQS_QUEUE_ARN'])
        self.orcomm.addQueue(os.environ['PREDICT_SQS_QUEUE_NAME'], os.environ['PREDICT_SQS_QUEUE_ARN'])
        self.orcomm.addTopic(os.environ['JOBS_NAME_TOPIC'], os.environ['JOBS_ARN_TOPIC'])
        self.timer = None
        self.intervalIsActive = False
        self.taskIsActive = False
        self.startTimer()


    def startTimer(self):
        print('>', 'Start listening for tasks...', flush=True)
        while True:
            gc.collect()
            time.sleep(2)
            self.getTask()
        
    
    def getTask(self):  # noqa: E501
        if self.taskIsActive == True:
            print('executing task...', flush=True)
        else:
            if os.environ['TASK'] == 'train-tml':
                items = self.orcomm.itemsForQueue(os.environ['TRAIN_SQS_QUEUE_NAME'], os.environ['TRAIN_SQS_QUEUE_ARN'], ['jobId', 'jobStatus', 'jobTask', 'order'], 1, False)
            elif os.environ['TASK'] == 'analyse-tml':
                items = self.orcomm.itemsForQueue(os.environ['PREDICT_SQS_QUEUE_NAME'], os.environ['PREDICT_SQS_QUEUE_ARN'], ['jobId', 'jobStatus', 'jobTask', 'order'], 1, False)  
            if not items:
                #print('Waiting for', os.environ['TASK'], 'task...', flush=True)
                pass
            else:
                print('Will start', os.environ['TASK'], 'task...', flush=True)
                self.taskIsActive = True
                item = items[0]
                task = self.postTask(job=item.MessageAttributes['jobId']['StringValue'], queueItem=item)

                if task['status']:
                    print('Task succeeded!', flush=True)
                    self.sendEventForFinishedJob(task['job'])
                else:
                    print('Task failed.', flush=True)
                    self.sendEventForFinishedJob(task['job'])
                try:
                    self.orcomm.getQueue(os.environ['PREDICT_SQS_QUEUE_ARN']).deleteItem(item.QueueUrl, item.ReceiptHandle)
                except Exception as e:
                    print(e, flush=True)
                

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
        if job.status == 'completed' or job.status == 'cancelled':
            print('Task already executed.', flush=True)
            self.taskIsActive = False
            return {'status': False, 'job': job, 'message': 'Task already executed.' }
        dataQuery = ("SELECT id, fileName, format, kind, label, location FROM Data WHERE id = %s")
        if job.task == 'train':
            dataSource = self.populateDataComplex(job, job.data_source, dataQuery, 'Data source is invalid.')
            if not dataSource:
                self.taskIsActive = False
                return {'status': False, 'job': job, 'message': 'Data source is invalid.' }
            else:
                job.data_source = dataSource
        else:
            dataSample = self.populateDataComplex(job, job.data_sample, dataQuery, 'Data sample is invalid.')
            if not dataSample:
                self.taskIsActive = False
                return {'status': False, 'job': job, 'message': 'Data sample is invalid.' }
            else:
                job.data_sample = dataSample
            model = self.populateDataComplex(job, job.model, dataQuery, 'Model is invalid.')
            if not model:
                self.taskIsActive = False
                return {'status': False, 'job': job, 'message': 'Model is invalid.' }
            else:
                job.model = model
        if job.kind == 'tml':
            status = self.execTML(job)
            self.taskIsActive = False
            return {'status': status, 'job': job, 'message': '' }
        else:
            return {'status': False, 'job': job, 'message': '' }
        
    
    def sendEventForFinishedJob(self, job):
        # create event
        jobDict = job.__dict__.copy()
        del jobDict['swagger_types']
        del jobDict['attribute_map']
        event = OREvent()
        event.TopicArn = os.environ['JOBS_ARN_TOPIC']
        event.Subject = 'Send Job'
        event.Message = json.dumps(jobDict)
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

    def execTML(self, job):
        tm = TopicModeller(self.s3)
        td = TopicDiscoverer()
        if job.task == 'train':
            start_time = time.time()
            print('will load dataset for training...', flush=True)
            csvData = self.downloadAndConvertCSV(job, job.data_source)
            print('will train model...', flush=True)
            try:
                self.updateJobStatus(job, 'training')
                vectorsBin = tm.CSV2Vectors(csvData, job.task_params)
                if not vectorsBin:
                    return self.cancellation(job, 'Wrong key.')    
            except Exception as e:
                print('001', flush=True)
                return self.cancellation(job, e)
            print('will upload model...', flush=True)
            self.persistModel(vectorsBin, job)
            self.updateJobStatus(job, 'completed')
            elapsed_time = time.time() - start_time
            print('Execution time max: ', elapsed_time, 'for job.id:', job.id,  flush=True)
        elif job.task == 'analyse':
            start_time = time.time()
            print('will load sample dataset for analysis...', flush=True)
            sampleCSV = self.downloadAndConvertCSV(job, job.data_sample)
            print('will load model...', flush=True)
            self.downloadAndStoreModel(job, job.model)
            print('will analyse data...', flush=True)
            self.updateJobStatus(job, 'analysing')
            vectorsBin = tm.CSV2Topics(sampleCSV, job.task_params)
            if not vectorsBin:
                return self.cancellation(job, 'Wrong key.')
            result = td.discover(vectorsBin)
            self.persistResult(job, result)
            self.updateJobStatus(job, 'completed')
            elapsed_time = time.time() - start_time
            print('Execution time max: ', elapsed_time, 'for job.id:', job.id,  flush=True) 
        tm = None
        return True
    

    def persistResult(self, job, result):
        inMemoryFile = io.BytesIO()
        inMemoryFile.write(json.dumps(result).encode())
        inMemoryFile.seek(0)   
        locationSplit = job.model['location'].split('/')
        bucketName = locationSplit[0]
        user = locationSplit[1]
        s3Resp = self.s3.uploadFileObject(inMemoryFile, bucketName, user + '/' + job.id + '_tml-result.json')
        print(s3Resp, flush=True)
        if s3Resp:
            dataset = DataComplex()
            dataset.id = str(uuid.uuid4())
            dataset.file_name = job.id + '_tml-result.json'
            dataset.location = bucketName + '/' + user + '/' + job.id + '_tml-result.json'
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
    

    def downloadAndStoreModel(self, job, target):
        locationSplit = target['location'].split('/')
        bucketName = locationSplit[0]
        key = locationSplit[1] + '/' + locationSplit[2]
        fileData = self.s3.downloadFile(bucketName, key)
        if not fileData:
            return self.cancellation(job, 'Problem downloading object from S3.')
        fileData.seek(0)
        with open('tmp/tml-model.sav', 'wb') as f:
            shutil.copyfileobj(fileData, f)
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

    
    def persistModel(self, vectorsBin, job):
        locationSplit = job.data_source['location'].split('/')
        bucketName = locationSplit[0]
        user = locationSplit[1]
        s3Resp = self.s3.uploadFileObject(vectorsBin, bucketName, user + '/' + job.id + '_tml-model.sav')
        if s3Resp:
            dataset = DataComplex()
            dataset.id = str(uuid.uuid4())
            dataset.file_name = job.id + '_tml-model.sav'
            dataset.location = bucketName + '/' + user + '/' + job.id + '_tml-model.sav'
            dataset.kind = 'model'
            dataset.format = 'application/octet-stream'
            dataset.label = 'Model created for user ' + user + ' as a result of a TML Training job.'  
            # store persistent data
            add_dataset = ("INSERT INTO Data "
                    "(id, fileName, format, kind, label, location) "
                    "VALUES (%s, %s, %s, %s, %s, %s)")
            data_dataset = (dataset.id, dataset.file_name, dataset.format, dataset.kind, dataset.label, dataset.location)
            self.db.add(add_dataset, data_dataset)