import os
import fs
import math
import fs.copy
import io
import gc
import time
import pathlib
import zipfile
import py7zr
import uuid
import csv
import json
from pathlib import Path
from shutil import rmtree
from aihandler.ai_tsk import TSK
from haystack import Finder
from haystack.indexing.cleaning import clean_wiki_text
from haystack.indexing.utils import convert_files_to_dicts, fetch_archive_from_http
from haystack.reader.farm import FARMReader
from haystack.reader.transformers import TransformersReader
from haystack.utils import print_answers
#from haystack.database.elasticsearch import ElasticsearchDocumentStore
from aihandler.es_docstore import ElasticsearchDocumentStore as ElasticsearchDocumentStore
from haystack.retriever.sparse import ElasticsearchRetriever
from ssl import create_default_context
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan
from pympler import tracker


class QNA(TSK):


    def __init__(self, db, s3, orcomm):
        TSK.__init__(self, db, s3, orcomm)
        self._taskKind = 'qna'
        self.useRemote = True
        self.index = None 
        self.startTimer()


    def execML(self, job):
        if job.task == 'analyse':
            start_time = time.time()
            self.index = job.data_sample['id']
            documentStore = self.connectElasticSearch(self.index, self.useRemote)
            if self.useRemote:
                if documentStore.client.indices.exists(index=self.index):
                    print('INFO: Index already stored.', flush=True)
                else:
                    print('INFO: Index will be created...', flush=True)
                    documentStore.client.indices.create(index = self.index)
            if 'selectTargetTextColumn' not in job.task_params and 'selectTargetIdColumn' not in job.task_params:
                self.updateJobStatus(job, 'cancelled')
                #return False
                return {'status': False, 'code': 'error', 'msg': 'ERROR: The keys selectTargetTextColumn & selectTargetIdColumn are missing.' }
            self.updateJobStatus(job, 'analysing')
            if self.isDatasetPersistent(job, documentStore, self.index):
                print('INFO: documents for index already stored.', flush=True)
            else:
                print('INFO: documents for index will be persisted...', flush=True)
                try:
                    print('INFO:', 'will download data...', flush=True)
                    sampleCSV = self.downloadAndConvertCSV(job, job.data_sample)
                    print('INFO:', 'will convert data...', flush=True)
                    sampleJSON = self.convertCSV2JSON4QnA(sampleCSV, job)
                    print('INFO:', 'will store data...', flush=True)
                    documentStore.write_documents(sampleJSON)
                    del sampleCSV
                    del sampleJSON
                except Exception as e:
                    self.updateJobStatus(job, 'cancelled')
                    #return False
                    return {'status': False, 'code': 'error', 'msg': 'ERROR: Check values for selectTargetTextColumn & selectTargetIdColumn.' }
            print('INFO:', 'will donwload and store model...', flush=True)
            self.downloadAndStoreZIPModel(job, job.model)
            #tr = tracker.SummaryTracker()
            print('INFO:', 'will set the retriever...', flush=True)
            retriever = self.setRetriever(documentStore) # noleak
            print('INFO:', 'will set the reader...', flush=True)
            reader = self.setReader(job)
            print('INFO:', 'will predict...', flush=True)
            result = self.setPrediction(reader, retriever, job)
            self.persistResult(job, result)
            self.updateJobStatus(job, 'completed')
            documentStore.client.transport.close()
            elapsed_time = time.time() - start_time
            print('Execution time max: ', elapsed_time, 'for job.id:', job.id,  flush=True)
            del retriever
            del reader
            del result
            del documentStore
            #tr.print_diff()
        elif job.task == 'train':
            start_time = time.time()
            pass
            elapsed_time = time.time() - start_time
            print('Execution time max: ', elapsed_time, 'for job.id:', job.id,  flush=True) 
        #return True
        return {'status': True, 'code': 'ok', 'msg': 'success' }


    def buildFilter(self, job):
        es_query_body = {
            "from": 0,
            "size" : 10,
            "query": {
                "bool": {
                    "should": []
                }
            }
        }
        mappings = json.loads(os.environ['DEFAULT_SAMPLES_MAPPINGS'])
        for m in mappings:
            for key in list(m.keys()):
                if m[key] == job.data_sample['id']:
                    es_query_body['query']['bool']['should'].append({ "match": { "external_source_id": m[key] } })
        return es_query_body


    def isDatasetPersistent(self, job, documentStore, index):
        '''
        bulk_deletes = []
        results = scan(self.documentStore.client,
            query=es_query_body,  # same as the search() body parameter
            index='document',
            doc_type='_doc',
            _source=False,
            track_scores=False,
            scroll='1s')
        for result in results:
            result['_op_type'] = 'delete'
            bulk_deletes.append(result)
        if len(list(results)) > 0:
            response = True
        bulk(self.documentStore.client, bulk_deletes)
        '''
        response = False
        es_query_body = self.buildFilter(job)
        results = documentStore.client.search(index=index, body=es_query_body, doc_type='_doc', scroll = '1s')
        if self.useRemote:
            if results['hits']['total'] > 0:
                response = True
        else: 
            if results['hits']['total']['value'] > 0:
                response = True
        return response
        

    def convertCSV2JSON4QnA(self, sampleCSV, job):
        sampleJSON = []
        for index, row in sampleCSV.iterrows():
            self.printProgressBar(index, len(sampleCSV.index), prefix = 'Progress:', suffix = 'Complete', length = 50)
            obj = {
                'text': row[job.task_params['selectTargetTextColumn']],
                'external_source_id': job.data_sample['id'],
                'meta': {
                    'dataset_id': job.data_sample['id'],
                    'user_id': job.user,
                    'document_id': row[job.task_params['selectTargetIdColumn']],
                }
            }
            if isinstance(obj['text'], str) == False:
                obj['text'] = 'undefined'
            sampleJSON.append(obj)
        return sampleJSON


    def connectElasticSearch(self, index, useRemote):
        if useRemote:
            context = create_default_context(cafile=os.environ['ELASTICSEARCH_PATHPEM'])
            return ElasticsearchDocumentStore(host=os.environ['ELASTICSEARCH_HOST'], 
                port=os.environ['ELASTICSEARCH_PORT'],
                scheme="https",
                username=os.environ['ELASTICSEARCH_USERNAME'], 
                password=os.environ['ELASTICSEARCH_PASSWORD'], 
                index=index,
                ssl_context=context)
        else:
            return ElasticsearchDocumentStore(host='elasticsearch', username='', password='', index=index)


    def setRetriever(self, documentStore):
        return ElasticsearchRetriever(document_store=documentStore)


    def setReader(self, job):
        return FARMReader(model_name_or_path='tmp/' + job.model['id'], use_gpu=False)


    def setPrediction(self, reader, retriever, job):
        finder = Finder(reader, retriever)
        if 'top_k_retriever' not in job.task_params:
            job.task_params['top_k_retriever'] = 10
        if 'top_k_reader' not in job.task_params:
            job.task_params['top_k_reader'] = 5
        results = []
        es_query_body = self.buildFilter(job)
        del es_query_body['from']
        del es_query_body['size']
        es_query_body = { 'external_source_id': ['ea13ebc0-18bf-4dfe-8750-61641fdbb00b'] } 
        for question in job.task_params['questions']:
            prediction = finder.get_answers(question=question, top_k_retriever=job.task_params['top_k_retriever'], top_k_reader=job.task_params['top_k_reader'], filters=None) #es_query_body['query']['bool']
            results.append(prediction)
        print('INFO:', results, flush=True)
        return results


    def printProgressBar (self, iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
        # Print New Line on Complete
        if iteration == total: 
            print()
