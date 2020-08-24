import connexion
import six

from task_module.models.health import Health  # noqa: E501
from task_module import util


def task_health_get():  # noqa: E501
    """task_health_get

    Check health of service. # noqa: E501


    :rtype: Health
    """
    return 'do some magic!'
