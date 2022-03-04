"""
Download SWARM data to CSV
"""
# standard library
import base64
import os
import json
# third party
import requests

token = os.environ.get('SWARM_API_TOKEN')
username = os.environ.get('SWARM_USER_NAME') or 'TheNatureConservancy'
password = os.environ.get('SWARM_PW')
url = 'https://bumblebee.hive.swarm.space/hive'


def login():
    data = {
        'username': username,
        'password': password
    }
    res = requests.post(url + '/login', data=data)
    return json.loads(res.content).get('token')

def get_time(value):
    return 0


def parse_data(value):
    return base64.b64decode(value).decode('utf-8')


def main():
    token = login()
    headers = {
        'Authorization': 'Bearer ' + token,
        'Accept': 'application/json'}
    res = requests.get(url + '/api/v1/messages', headers=headers)
    print(res.content)
    for item in json.loads(res.content):
        for key, value in item.items():
            if key == 'data':
                value = parse_data(value)
                print(value, end=', ')
        print()


if __name__ == "__main__":
    main()
