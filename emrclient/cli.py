#!/usr/bin/env python3
'''
Helper script to list applications and kill applications from a remote EMR. Also may add steps to EMR cluster
'''

import json
from os.path import expanduser
import os.path
import re
import datetime

import click
from tabulate import tabulate

from config import Config
from yarncli import YarnClient

import boto3

cache_address = expanduser("~") + "/.emrclient"


def get_cache_file_location():
    return cache_address


def set_cache_file_location(cache_address_overwrite):
    cache_address = cache_address_overwrite


@click.group()
def cli():
    pass


@cli.command()
@click.option('-m', '--master-address', help='ip:port of master web api. Default\'s port to 8088 if no port given')
@click.option('-b', '--s3-bucket', help='s3 bucket to store the spark job jars')
@click.option('-r', '--region', help='Region of cluster. e.g. eu-west-1')
@click.option('-c', '--cluster-id', help='String value of the cluster id')
def configure(master_address, s3_bucket, region, cluster_id):
    if os.path.exists(get_cache_file_location()):

        with open(get_cache_file_location(), "r") as file_contents:
            json_contents = json.load(file_contents)

            if not master_address:
                master_address = json_contents['master_address']
            else:
                master_address = normalise_master_address(master_address)

            if not s3_bucket:
                s3_bucket = json_contents['s3_bucket']

            if not region:
                region = json_contents['region']

            if not cluster_id:
                cluster_id = json_contents['cluster_id']

            emr_client_config = Config(master_address, s3_bucket, region, cluster_id)
            file_contents.close()
    else:
        master_address = normalise_master_address(master_address)
        emr_client_config = Config(master_address, s3_bucket, region, cluster_id)

    emr_cache_file = open(get_cache_file_location(), "w")

    emr_cache_file.write(emr_client_config.to_JSON())
    emr_cache_file.close()


def normalise_master_address(master_address):
    ends_with_port = re.compile('.*?:\n+$')
    starts_with_http = re.compile('^http://.*?')

    if not ends_with_port.match(master_address):
        master_address += ":8088"

    if not starts_with_http.match(master_address):
        master_address = "http://" + master_address

    return master_address


@cli.command()
@click.option('-m', '--master-address', help='Overwrite the address of master web api. Default port 8088. Not cached')
def list_applications_running(master_address):
    list_applications_by_state(master_address, 'RUNNING')


@cli.command()
@click.argument('state')
@click.option('-m', '--master-address', help='Overwrite the address of master web api. Default port 8088. Not cached')
def list_applications(master_address, state):
    list_applications_by_state(master_address, state)


def list_applications_by_state(master_address, state):
    yarn_client = build_yarn_client(master_address)
    result = yarn_client.list_by_state(state)
    print(tabulate(result[0], result[1], tablefmt='plain'))


def build_yarn_client(master_address):
    if master_address:
        yarn_client = YarnClient(normalise_master_address(master_address))
    else:
        with open(get_cache_file_location(), "r") as file_contents:
            json_contents = json.load(file_contents)
            yarn_client = YarnClient(json_contents['master_address'])
    return yarn_client


def normalise_time(time):
    if not time == 0:
        return datetime.datetime.fromtimestamp(time / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    return '-'


@cli.command()
@click.argument('application_id')
@click.option('-m', '--master-address', help='Overwrite the address of master web api. Default port 8088. Not cached')
def kill(application_id, master_address):
    yarn_client = build_yarn_client(master_address)
    if yarn_client.kill(application_id):
        print("successfully killed")


def get_config(s3_bucket, region, cluster_id):
    with open(get_cache_file_location(), "r") as file_contents:
        json_contents = json.load(file_contents)

        if not s3_bucket:
            s3_bucket_to_use = json_contents['s3_bucket']
        else:
            s3_bucket_to_use = s3_bucket

        if not region:
            region_to_use = json_contents['region']
        else:
            region_to_use = region

        if not cluster_id:
            cluster_id_to_use = json_contents['cluster_id']
        else:
            cluster_id_to_use = cluster_id

        return Config(json_contents['master_address'], s3_bucket_to_use, region_to_use, cluster_id_to_use)


@cli.command()
@click.option('-c', '--cluster-id', help='Overwrite region of cluster. Not cached')
@click.option('-r', '--region', help='Overwrite region of cluster. Not cached')
# 'PENDING'|'RUNNING'|'COMPLETED'|'CANCELLED'|'FAILED'|'INTERRUPTED'
@click.argument('state')
def list_steps(cluster_id, region, state):
    config = get_config(None, region, cluster_id)

    emr_client = boto3.client('emr', region_name=config.region)
    result = emr_client.list_steps(ClusterId=config.cluster_id, StepStates=[state])

    headers = ['Id', 'Name', 'Creation-Time', 'Start-Time', 'Finished-Time', 'Config']
    data = []

    for app in result['Steps']:

        creationTime = '-'
        startTime = '-'
        finishedTime = '-'

        if 'CreationDateTime' in app['Status']['Timeline']:
            creationTime = app['Status']['Timeline']['CreationDateTime']

        if 'StartDateTime' in app['Status']['Timeline']:
            startTime = app['Status']['Timeline']['StartDateTime']

        if 'EndDateTime' in app['Status']['Timeline']:
            finishedTime = app['Status']['Timeline']['EndDateTime']

        data.append([app['Id'], app['Name'],
                     creationTime,
                     startTime,
                     finishedTime,
                     app['Config']])

    print(tabulate(data, headers, tablefmt='plain'))

@cli.command()
@click.option('-f', '--file', help='Upload the file. This will be uploaded to s3 and overwrite whatever is there')
@click.option('-b', '--s3-bucket',
              help='Overwrite the s3 bucket location for the file to be uploaded to. Does not get cached')
@click.option('-s', '--s3-file', help='s3 file for the job. Used if already uploaded')
@click.option('-c', '--cluster-id', help='Overwrite region of cluster. Not cached')
@click.argument('name')
@click.argument('main_class')
@click.option('-r', '--region', help='Overwrite region of cluster. Not cached')
@click.option('-a', '--args', help='arguments for jar')
def submit_job(cluster_id, name, args, file, s3_bucket, s3_file, main_class, region):
    jar_to_use = ''

    config = get_config(s3_bucket, region, cluster_id)

    if file:
        print('uploading file ' + file + ' to s3 bucket ' + config.s3_bucket)
        s3_client = boto3.client('s3')
        file_name = os.path.basename(file)
        jar_to_use = "s3://" + config.s3_bucket + "/" + file_name
        s3_client.upload_file(file, s3_bucket, file_name)
    elif s3_file:
        print('Using file in s3 ' + s3_file)
        jar_to_use = s3_file

    emr_client = boto3.client('emr', region_name=config.region)

    response = emr_client.add_job_flow_steps(
        JobFlowId=config.cluster_id,
        Steps=[
            {
                'Name': name,
                # TERMINATE_JOB_FLOW'|'TERMINATE_CLUSTER'|'CANCEL_AND_WAIT'|'CONTINUE'
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '--deploy-mode', 'cluster', '--class', main_class,
                             jar_to_use] + args.split(',')
                }
            },
        ]
    )

    print(response)


def main():
    cli()


if __name__ == '__main__':
    main()
