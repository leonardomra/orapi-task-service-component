# coding: utf-8

import sys
from setuptools import setup, find_packages

NAME = "task_module"
VERSION = "1.0.0"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

REQUIRES = ["connexion"]

setup(
    name=NAME,
    version=VERSION,
    description="Task.ai API",
    author_email="contact@openresearch.cloud",
    url="",
    keywords=["Swagger", "Task.ai API"],
    install_requires=REQUIRES,
    packages=find_packages(),
    package_data={'': ['swagger/swagger.yaml']},
    include_package_data=True,
    entry_points={
        'console_scripts': ['task_module=task_module.__main__:main']},
    long_description="""\
    The OpenResearch API (OR-API) provides users with machine learning-powered NLP tools for scientific text analysis and exploration. In addition to allowing the training of custom models with custom data, the OR-API enables users to integrate the insights from the analysis into dashboards and applications.
    """
)
