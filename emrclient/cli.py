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

cache_file_location = expanduser("~") + "/.emrclient"


@click.group()
def cli():
    pass


@cli.command()
@click.option('-m', '--master-address', help='ip:port of master web api. Default\'s port to 8088 if no port given')
@click.option('-b', '--s3-bucket', help='s3 bucket to store the spark job jars')
def configure(master_address, s3_bucket):
    if os.path.exists(cache_file_location):

        with open(cache_file_location, "r") as file_contents:
            json_contents = json.load(file_contents)

            if not master_address:
                master_address = json_contents['master']
            else:
                master_address = normalise_master_address(master_address)

            if not s3_bucket:
                s3_bucket = json_contents['s3_bucket']

            emr_client_config = Config(master_address, s3_bucket)
            file_contents.close()
    else:
        master_address = normalise_master_address(master_address)
        emr_client_config = Config(master_address, s3_bucket)

    emr_cache_file = open(cache_file_location, "w")

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
def list_running(master_address):
    list_by_state(master_address, 'RUNNING')

@cli.command()
@click.argument('state')
@click.option('-m', '--master-address', help='Overwrite the address of master web api. Default port 8088. Not cached')
def list(master_address, state):
    list_by_state(master_address, state)

def list_by_state(master_address, state):

    if master_address:
        yarn_client = YarnClient(normalise_master_address(master_address))
    else:
        with open(cache_file_location, "r") as file_contents:
            json_contents = json.load(file_contents)
            yarn_client = YarnClient(json_contents['master_address'])

    result = yarn_client.list_by_state(state)

    print(tabulate(result[0], result[1], tablefmt='plain'))


def normalise_time(time):
    if not time == 0:
        return datetime.datetime.fromtimestamp(time / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    return '-'


@cli.command()
@click.argument('application_id')
def kill(application_id):
    print('application_id')
    # command = yarn_pre_application_command + " -kill " + application_id
    # print("executing command..." + command)
    # os.system(command)


def main():
    cli()


if __name__ == '__main__':
    main()
