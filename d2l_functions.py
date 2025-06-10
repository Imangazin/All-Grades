import requests
from requests.auth import HTTPBasicAuth
import os
import urllib.parse
from logger_config import logger
import dotenv

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

def trade_in_refresh_token(config):
    try:
        response = requests.post(
            'https://auth.brightspace.com/core/connect/token',
            data={
                'grant_type': 'refresh_token',
                'refresh_token': config['refresh_token'],
                'scope': config['scope']
            },
            auth=HTTPBasicAuth(config['client_id'], config['client_secret'])
        )
        response.raise_for_status()
        response_data = response.json()
        return response_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during token refresh: {e}")
        return None
    except KeyError:
        logger.error("Error: Unexpected response format.")
        return None

# d2l GET call
def get_with_auth(endpoint, access_token, stream=False):
    try:
        headers = {'Authorization': f'Bearer {access_token}'}
        response = requests.get(endpoint, headers=headers, stream=stream)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during GET request: {e}")
        return None


def post_with_auth(endpoint, access_token, data=None):
    try:
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        response = requests.post(endpoint, headers=headers, json=data)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during POST request: {e}")
        return None
