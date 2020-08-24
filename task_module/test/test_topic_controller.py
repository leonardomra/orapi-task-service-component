# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from task_module.models.health import Health  # noqa: E501
from task_module.test import BaseTestCase


class TestTopicController(BaseTestCase):
    """TopicController integration test stubs"""

    def test_task_topic_confirm_post(self):
        """Test case for task_topic_confirm_post

        
        """
        body = 'body_example'
        headers = [('x_amz_sns_message_type', 'x_amz_sns_message_type_example'),
                   ('x_amz_sns_message_id', 'x_amz_sns_message_id_example'),
                   ('x_amz_sns_topic_arn', 'x_amz_sns_topic_arn_example')]
        response = self.client.open(
            '/OR-API/task-api/1.0.0/task/topic/confirm',
            method='POST',
            data=json.dumps(body),
            headers=headers,
            content_type='text/plain')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
