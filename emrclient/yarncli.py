import datetime
from datetime import timedelta

import requests

def normalise_time(time):
    if not time == 0:
        return datetime.datetime.fromtimestamp(time / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    return '-'


class YarnClient:
    def __init__(self, master_address):
        self.master_address = master_address

    def list_by_state(self, state):
        headers = ['Started-Time', 'Finished-Time', 'Application-Id', 'Application-Name', 'Application-Type', 'User',
                   'Queue', 'State', 'Final-State', 'Elapsed-Time', 'Progress', 'Tracking-URL']
        data = []
        response = requests.get(self.master_address + '/ws/v1/cluster/apps?state=' + state)
        if response._content and 'apps' in response.json():
            for app in response.json()['apps']['app']:
                data.append([normalise_time(app['startedTime']),
                             normalise_time(app['finishedTime']),
                             app['id'], app['name'], app['applicationType'],
                             app['user'], app['queue'], app['state'],
                             app['finalStatus'],
                             timedelta(seconds=app['elapsedTime'] / 1000.0),
                             '{:}%'.format(app['progress']),
                             app['trackingUrl']])

        return data, headers

    def kill(self, application_id):
        url_endpoint = self.master_address + '/ws/v1/cluster/apps/' + application_id + "/state"
        response = requests.put(url=url_endpoint,
                                json={'state': 'KILLED'})
        return response.status_code == requests.codes.ok
