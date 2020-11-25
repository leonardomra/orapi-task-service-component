import os
import io
import gc
import json
import time
import uuid
import shutil
import sys
import pandas as pd
from aihandler.ai_tsk import TSK
from aihandler.tml.topicdiscoverer import TopicDiscoverer
from aihandler.tml.topicmodeller import TopicModeller
from threading import Timer
from task_module.models.job import Job
from task_module.models.datacomplex import DataComplex
from orcommunicator.orevent import OREvent


class TML(TSK):

    def __init__(self, db, s3, orcomm):
        self._taskKind = 'tml'
        self.db = db
        self.s3 = s3
        self.orcomm = orcomm
        self.timer = None
        self.intervalIsActive = False
        self.taskIsActive = False
        self.startTimer()



    def execML(self, job):
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
                    #return self.cancellation(job, 'Wrong key.')
                    return {'status': self.cancellation(job, 'Wrong key.'), 'code': 'error', 'msg': 'ERROR: Wrong key.' }
            except Exception as e:
                #return self.cancellation(job, e)
                return {'status': self.cancellation(job, e), 'code': 'error', 'msg': e } 
            print('will upload model...', flush=True)
            self.persistTMLModel(vectorsBin, job)
            self.updateJobStatus(job, 'completed')
            elapsed_time = time.time() - start_time
            print('Execution time max: ', elapsed_time, 'for job.id:', job.id,  flush=True)
            del csvData
        elif job.task == 'analyse':
            start_time = time.time()
            print('will load sample dataset for analysis...', flush=True)
            sampleCSV = self.downloadAndConvertCSV(job, job.data_sample)
            print('will load model...', flush=True)
            self.downloadAndStoreTMLModel(job, job.model)
            print('will analyse data...', flush=True)
            self.updateJobStatus(job, 'analysing')
            vectorsBin = tm.CSV2Topics(sampleCSV, job.task_params)
            if not vectorsBin:
                return {'status': self.cancellation(job, 'Wrong key.'), 'code': 'error', 'msg': 'ERROR: Wrong key.' }
            result = td.discover(vectorsBin)
            self.persistResult(job, result)
            self.updateJobStatus(job, 'completed')
            elapsed_time = time.time() - start_time
            print('Execution time max: ', elapsed_time, 'for job.id:', job.id,  flush=True)
            del sampleCSV
            del vectorsBin
            del result
        del tm
        del td
        return {'status': True, 'code': 'ok', 'msg': 'success' }