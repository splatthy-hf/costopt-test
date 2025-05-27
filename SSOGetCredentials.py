'''
This script is set up to manage the SSO authentication for users as an improvement over the existing auth scripts

When executed with main it will check to see if you have a valid SSO login.  If not it will prompt you to refresh your auth token
Invoke just the get_access_token function if you only want to refresh your token (and not update your config map)
get_account_names will return a list of all account Names that could be used to iterate through
This file will also hydrate your aws config file, with a profile for each account
    If you have multiple IAM roles for an account, the first time it sees a new account it will prompt you to pick which role you want to use
Default Usage:
    from SSOGetCredentials import sso_login
    Then somewhere in your code, just call sso_login()
'''

import socket
import json
import os
from datetime import datetime, timedelta
from time import sleep
import configparser
import hashlib
import pytz
import boto3
import streamlit as st

START_URL = 'https://d-90670a4e60.awsapps.com/start'
SSO_DIR = os.path.expanduser('~/.aws/sso/cache')
CACHE = hashlib.sha1(START_URL.encode("utf-8")).hexdigest()

PATH = f'{SSO_DIR}/{CACHE}.json'

def check_token_time(now=datetime.now(pytz.UTC), data={}):
    
#    json_files = [pos_json for pos_json in os.listdir(SSO_DIR) if pos_json.endswith('.json')]
#    if len(json_files) > 0:
#        for json_file in json_files :
    os.makedirs(SSO_DIR, exist_ok=True) 
    now = datetime.now(pytz.UTC)  
    accesstoken = ''
    if not os.path.isfile(PATH):
        data['startUrl'] = "https://d-90670a4e60.awsapps.com/start"
        data['region'] = "us-east-1"
        data['accessToken'] = accesstoken
        data['expiresAt'] = ''
        data['clientId'] = ''
        data['clientSecret'] = ''
        data['registrationExpiresAt'] = ''
    else:
        with open(PATH, 'r', encoding='utf-8') as file :
            data = json.load(file)
            if data.get('expiresAt') is not None:
                dt = datetime.fromisoformat(data['expiresAt'])
                if  now < dt:
                    accesstoken = data['accessToken'] 

    return accesstoken

def get_access_token(webui=False):
    '''
    Attempt to retrieve the access token from the SSO Cache
    If the token is still valid, return it
    If the token is not found, or expired, generate a new token
    '''
    host = socket.gethostname()
    now = datetime.now(pytz.UTC)
    data = {}
    accesstoken = check_token_time(now, data)
    if accesstoken == '':
        sso_oidc = boto3.client('sso-oidc', 'us-east-1')
        client_creds = sso_oidc.register_client(
            clientName=host,
            clientType='public',
        )
        data['clientId'] = client_creds['clientId']
        data['clientSecret'] = client_creds['clientSecret']
        regexp_iso = datetime.fromtimestamp(client_creds['clientSecretExpiresAt'], pytz.UTC)
        data['registrationExpiresAt'] = regexp_iso.strftime("%Y-%m-%dT%H:%M:%SZ")
        device_authorization = sso_oidc.start_device_authorization(
            clientId=client_creds['clientId'],
            clientSecret=client_creds['clientSecret'],
            startUrl=START_URL,
        )
        url = device_authorization['verificationUriComplete']
        device_code = device_authorization['deviceCode']
        expires_in = device_authorization['expiresIn']
        interval = device_authorization['interval']
        if not webui:
            print('Not authenticated, please visit the following site to auth: ' + url)
        else:
            st.write('Not authenticated, please visit the following site to auth: ' + url)
        for n in range(1, expires_in // interval + 1):
            sleep(interval)
            try:
                r = sso_oidc.create_token(
                    grantType='urn:ietf:params:oauth:grant-type:device_code',
                    deviceCode=device_code,
                    clientId=client_creds['clientId'],
                    clientSecret=client_creds['clientSecret'],
                )
                expire_obj = now + timedelta(seconds = r['expiresIn'])
                data['expiresAt'] = expire_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
                accesstoken = r['accessToken']
                data['accessToken'] = accesstoken
                with open(PATH, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)                
                break
            except sso_oidc.exceptions.AuthorizationPendingException:
                pass
    return accesstoken

def update_config_files(accesstoken):
    '''
    Using the access token for SSO identify all Accounts the user has access to
    For each account check to see if a Config file entry exists
    If it does not look at how many roles the person has in that account
    If only 1, then create a config for it
    If more than 1, if FullAdmin exists bind that
    Otherwise gives the user a prompt to pick what role they want
    Subsequent runs will not make changes unless an account is not present
    '''
    START_URL = 'https://d-90670a4e60.awsapps.com/start'

    config_file = os.path.expanduser('~/.aws/config')

    config = configparser.RawConfigParser()
    config.read(config_file)
    client = boto3.client('sso', 'us-east-1')

    account_list = get_accounts(accesstoken)
    for account in account_list:
        r2 = client.list_account_roles(
            accessToken=accesstoken,
            accountId=account['accountId']
        )
        configpath = 'profile ' + aname_sanitizer(account['accountName'])
        if not config.has_section(configpath):
            config.add_section(configpath)
            st.write('New Account Mapping found for account ' + account['accountId'] + ': ' + aname_sanitizer(account['accountName']))
            if len(r2['roleList']) > 1:
                roles = []
                for role in r2['roleList']:
                    roles.append(role['roleName'])
                if 'FullAdmin' in roles:
                    selectedrole = 'FullAdmin'
                else:
                    st.write('You have Multiple Roles for this account: ' + roles)
                    selectedrole = input('Copy/Paste the Rolename do you want in config: ')
                    while selectedrole not in roles:
                        selectedrole = input('INVALID ROLENAME - Which Rolename do you want in config: ')
            else:
                selectedrole = r2['roleList'][0]['roleName']
            config.set(configpath, 'sso_START_URL', START_URL)
            config.set(configpath, 'sso_account_id', account['accountId'])
            config.set(configpath, 'sso_account_name', account['accountName'])
            config.set(configpath, 'sso_role_name', selectedrole)
            config.set(configpath, 'sso_region', 'us-east-1')
            # Write the updated config file
            with open(config_file, 'w+', encoding='utf-8') as f:
                config.write(f)

def get_accounts(accesstoken=None):
    '''
    Get a list of all the accounts available
    Returns a list of dictionaries
    Dictionaries contain accountId, accountName, emailAddress
    '''
    if accesstoken is None:
        accesstoken = get_access_token()
    # Create the client
    client = boto3.client('sso', 'us-east-1')

    # Get the paginator for the list_accounts operation
    paginator = client.get_paginator('list_accounts')

    # Create a PageIterator from the paginator
    page_iterator = paginator.paginate(
        accessToken=accesstoken
    )

    # Initialize an empty list to store the accounts
    accounts = []
    # Iterate over the pages and append the accounts to the list
    for page in page_iterator:
        accounts.extend(page['accountList'])
    
    for account in accounts:
        account['accountName'] = aname_sanitizer(account['accountName'])

    return accounts

def get_account_names():
    '''
    Return a list of all Account Names that are in AWS SSO
    '''
    account_list = get_accounts(get_access_token())
    anames = []
    anames = [account['accountName'] for account in account_list]

    return anames

def aname_sanitizer(aname):
    '''
    Take an account name as an input, and return a sanitized version for a config file entry'''
    aname = str(aname)
    return aname.replace(" ", "_")

def map_accountid_to_name(accounts, accountid):
    '''
    Take an account ID as an input, and return the appropriate AWS account name'''
    for account in accounts:
        if account['accountId'] == accountid:
            return aname_sanitizer(account['accountName'])
    return(accountid)

#Look at way to preserve in memory the account listing to avoid repeated API calls

def sso_login(webui=False):

    '''
    Check to see if SSO token is valid
    Once logged in, ensure a config entry exists for all accounts the person has access to
    '''
    accesstoken = get_access_token(webui)
    if accesstoken != '':
        update_config_files(accesstoken)
        return True
    else:
        return False
    
if __name__ == "__main__":
    sso_login()