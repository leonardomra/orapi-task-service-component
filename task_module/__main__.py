#!/usr/bin/env python3
import sys
import os
import connexion
from task_module import encoder
#from aihandler.ai_tml import TML
#from aihandler.ai_qna import QNA
from aihandler.ai_ner import NER
from dbhandler.mysql_handler import MySQLHandler
from s3handler.s3_handler import S3Handler
from orcomm_module.orcommunicator import ORCommunicator
from dotenv import load_dotenv


def main():
    print(sys.path)
    load_dotenv()
    app = connexion.App(__name__, specification_dir='./swagger/')
    app.app.json_encoder = encoder.JSONEncoder
    app.add_api('swagger.yaml', arguments={'title': 'Task.ai API'}, pythonic_params=True)
    with app.app.app_context():
        db = MySQLHandler(os.environ['MYSQL_USER'], os.environ['MYSQL_PASSWORD'], os.environ['MYSQL_HOST'], os.environ['MYSQL_DATABASE'])
        s3 = S3Handler(os.environ['AWS_REGION'], os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'])
        orcomm = ORCommunicator(os.environ['AWS_REGION'], os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'])
        orcomm.addQueue(os.environ['TRAIN_SQS_QUEUE_NAME_TML'], os.environ['TRAIN_SQS_QUEUE_ARN_TML'])
        orcomm.addQueue(os.environ['PREDICT_SQS_QUEUE_NAME_TML'], os.environ['PREDICT_SQS_QUEUE_ARN_TML'])
        orcomm.addQueue(os.environ['TRAIN_SQS_QUEUE_NAME_QNA'], os.environ['TRAIN_SQS_QUEUE_ARN_QNA'])
        orcomm.addQueue(os.environ['PREDICT_SQS_QUEUE_NAME_QNA'], os.environ['PREDICT_SQS_QUEUE_ARN_QNA'])
        orcomm.addQueue(os.environ['TRAIN_SQS_QUEUE_NAME_NER'], os.environ['TRAIN_SQS_QUEUE_ARN_NER'])
        orcomm.addQueue(os.environ['PREDICT_SQS_QUEUE_NAME_NER'], os.environ['PREDICT_SQS_QUEUE_ARN_NER'])
        orcomm.addTopic(os.environ['JOBS_NAME_TOPIC'], os.environ['JOBS_ARN_TOPIC'])
        
        if os.environ['TASK'] == 'train-tml' or os.environ['TASK'] == 'analyse-tml':
            #from aihandler.ai_tml import TML
            #TML(db, s3, orcomm)
            pass
        elif os.environ['TASK'] == 'train-qna' or os.environ['TASK'] == 'analyse-qna':
            #from aihandler.ai_qna import QNA
            #QNA(db, s3, orcomm)
            pass
        elif os.environ['TASK'] == 'train-ner' or os.environ['TASK'] == 'analyse-ner':
            from aihandler.ai_ner import NER
            NER(db, s3, orcomm)
        

    app.run(port=80)


if __name__ == '__main__':
    main()
