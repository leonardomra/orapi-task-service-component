import connexion
import six
import os
from task_module.models.tasks import Tasks
from task_module import util
#from ai.tml.topicdiscoverer import TopicDiscoverer
#from ai.tml.topicmodeller import TopicModeller
from aihandler.ai import AIntel
#from s3handler.s3_handler import S3Handler

#s3 = S3Handler(os.environ['AWS_REGION'], os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'])
#tm = TopicModeller(s3)
#td = TopicDiscoverer()
ai = AIntel()

def task_get():  # noqa: E501
    """task_get

    Obtain information about tasks. # noqa: E501


    :rtype: List[Tasks]
    """
    return 'do some magic!'


def task_post(job=None):  # noqa: E501
    """task_post

    Create a task. # noqa: E501

    :param job: Id of Job.
    :type job: 

    :rtype: List[Tasks]
    """
   
    return 'do some magic!'