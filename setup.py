from distutils.core import setup
import setuptools
from setuptools.command.test import test as TestCommand
from setuptools import setup

setup(
    name='emrclient',
    packages=['emrclient'],
    version='0.1',
    description='This is created to view and kill applications/jobs running on yarn inside Amazon EMR or any other remote location. Currently the amazon api does not include stopping jobs. Also supports deploying steps',
    author='Mark Kelly',
    author_email='mkelly28@tcd.ie',
    url='https://github.com/zalando/emrclient',
    download_url='https://github.com/zalando/emrclient/tarball/0.0.1',
    keywords=['emr', 'aws', 'yarn', 'kill', 'view', 'client', 'spark', 'api'],
    classifiers=[],
    entry_points={'console_scripts': 'emrclient = emrclient.cli:main'},
    tests_require=['pytest'],
    test_suite='tests',
    # packages=setuptools.find_packages(exclude=['tests', 'tests.*']),
    install_requires=['clickclick','tabulate', 'boto3'],
    license='Apache License 2.0'
)
