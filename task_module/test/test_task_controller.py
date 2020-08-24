# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from task_module.models.tasks import Tasks  # noqa: E501
from task_module.test import BaseTestCase


class TestTaskController(BaseTestCase):
    """TaskController integration test stubs"""

    def test_task_get(self):
        """Test case for task_get

        
        """
        response = self.client.open(
            '/OR-API/task-api/1.0.0/task',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_task_post(self):
        """Test case for task_post

        
        """
        headers = [('job', '38400000-8cf0-11bd-b23e-10b96e4ef00d')]
        response = self.client.open(
            '/OR-API/task-api/1.0.0/task',
            method='POST',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
