__author__ = 'Andrew Shuler: ashuler[at]relationshipvelocity.com'

import json
import logging

import requests as r


class Connection(object):
    def __init__(self, username, password, grant_type, client_id, client_secret, sandbox=True):
        # Logging setup
        self.log = logging.getLogger(__name__)
        # create non-JSON dict object with all other configuration params.
        payload = {
            'grant_type': grant_type,
            'client_id': client_id,
            'client_secret': client_secret,
            'username': username,
            'password': password
        }
        if sandbox:
            login_url = 'https://test.salesforce.com/services/oauth2/token'
        else:
            login_url = 'https://login.salesforce.com/services/oauth2/token'
        # start session.
        self.session = r.session()
        # authenticate into SF.
        login = self.session.post(
            login_url,
            data=payload,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=None
        ).json()
        # upon successful authentication, get instance URL and access token from the login response JSON.
        try:
            self.instance_url = login['instance_url']
            self.base_url = self.instance_url + '/services/data'
            # update Authorization session header with value "Bearer {access_token}"
            self.session.headers.update(
                {'Authorization': f'Bearer {login["access_token"]}'})
            self.log.info(f'Successfully connected to {self.instance_url}.')
        except Exception as login_error:
            self.log.exception(
                f'Failed to connect to SF instance.\nError: {login_error}')
            raise
        # get the latest valid Salesforce API Version.
        version = self.session.get(self.base_url).json()[-1]['version']
        # extend the base_url to include the standard API path prefix and the version number.
        self.base_url += f'/v{version}'

    def soql_query(self, query_string):
        """ Response JSON has 3 keys: totalSize, done, records. """
        try:
            # Encode and execute soql query
            raw_query = self.base_url + f'/query/?q={query_string}'
            enc_query = raw_query #r.utils.requote_uri(raw_query)
            self.log.debug(enc_query)
            results = self.session.get(enc_query).json()
            if int(results['totalSize']) > 0:
                records = [
                    {key: value for key, value in record.items() if key !=
                     'attributes'}
                    for record in results['records']
                ]
                while 'nextRecordsUrl' in results:
                    next_url = results['nextRecordsUrl']
                    results = self.session.get(
                        self.instance_url + next_url).json()
                    # self.log.debug(f'Results: {results}')
                    records.extend(
                        {key: value for key, value in record.items() if key !=
                         'attributes'}
                        for record in results['records']
                    )
            else:
                records = []
            self.log.info(f'SOQL query returned {len(records)} records.')
            return records
        except Exception as soql_err:
            self.log.exception(
                f'Failed to execute SOQL query "{query_string}".\nError: {soql_err}')
            raise

    def describe_fields(self, sobject, print_keys=False):
        try:
            fields = [
                'autoNumber',
                'calculated',
                'compoundFieldName',
                'controllerName',
                'createable',
                'custom',
                'externalId',
                'label',
                'mask',
                'name',
                'nameField',
                'nillable',
                'referenceTo',
                'relationshipName',
                'updateable'
            ]

            return self.describe_object(sobject, 'fields', fields, print_keys)
        except Exception as describe_err:
            self.log.exception(
                f'Failed to describe "{sobject}".\nError: {describe_err}')
            raise

    def describe_object(self, sobject, key, fields=[], print_keys=False):
        try:
            details = self.session.get(
                self.base_url + '/sobjects/{}/describe'.format(sobject)).json()[key]
            if print_keys:
                self.log.info(f'{key} keys\n\t{details[0].keys()}')
            for record in details:
                [record.pop(_key) for _key in list(record.keys())
                 if fields and _key not in fields]
                record.update({'sobject': sobject})
            return details
        except Exception as describe_err:
            raise ValueError(
                f'Failed to describe "{sobject}".\nError: {describe_err}')

    def get_response(self, url):
        try:
            session_url = self.base_url + url
            self.log.debug(f'URL: {session_url}')
            results = self.session.get(session_url)

            return results
        except Exception as describe_err:
            self.log.exception(
                f'Failed to get results.\nError: {describe_err}')
            raise

    def close_connection(self):
        self.session.close()
        self.log.info('Closed connection to Salesforce REST API')
