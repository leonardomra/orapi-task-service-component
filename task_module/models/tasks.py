# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from task_module.models.base_model_ import Model
from task_module import util


class Tasks(Model):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """
    def __init__(self, job: str=None):  # noqa: E501
        """Tasks - a model defined in Swagger

        :param job: The job of this Tasks.  # noqa: E501
        :type job: str
        """
        self.swagger_types = {
            'job': str
        }

        self.attribute_map = {
            'job': 'job'
        }
        self._job = job

    @classmethod
    def from_dict(cls, dikt) -> 'Tasks':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The Tasks of this Tasks.  # noqa: E501
        :rtype: Tasks
        """
        return util.deserialize_model(dikt, cls)

    @property
    def job(self) -> str:
        """Gets the job of this Tasks.


        :return: The job of this Tasks.
        :rtype: str
        """
        return self._job

    @job.setter
    def job(self, job: str):
        """Sets the job of this Tasks.


        :param job: The job of this Tasks.
        :type job: str
        """

        self._job = job
