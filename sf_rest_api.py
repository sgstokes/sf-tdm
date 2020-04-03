__author__ = 'Andrew Shuler: ashuler[at]relationshipvelocity.com'

import json
import requests as r


class Connection(object):
    def __init__(self, username, password, grant_type, client_id, client_secret, sandbox=True):
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
            headers={'Content_Type': 'application/x-www-form-urlencoded'},
            timeout=None
        ).json()
        # upon successful authentication, get instance URL and access token from the login response JSON.
        try:
            self.instance_url = login['instance_url']
            self.base_url = self.instance_url + '/services/data'
            # update Authorization session header with value "Bearer {access_token}"
            self.session.headers.update(
                {'Authorization': f'Bearer {login["access_token"]}'})
            print(f'Successfully connected to {self.instance_url}.')
        except Exception as login_error:
            print(f'Failed to connect to SF instance.\nError: {login_error}')
        # get the latest valid Salesforce API Version.
        version = self.session.get(self.base_url).json()[-1]['version']
        # extend the base_url to include the standard API path prefix and the version number.
        self.base_url += f'/v{version}'

    def soql_query(self, query_string):
        """ Response JSON has 3 keys: totalSize, done, records. """
        try:
            # replace spaces with "+" and execute soql query
            results = self.session.get(
                self.base_url + f'/query/?q={query_string}'.replace(' ', '+')).json()
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
                    # print(f'Results: {results}')
                    records.extend(
                        {key: value for key, value in record.items() if key !=
                         'attributes'}
                        for record in results['records']
                    )
            else:
                records = []
            print(f'SOQL query returned {len(records)} records.')
            return records
        except Exception as soql_err:
            raise ValueError(
                f'Failed to execute SOQL query "{query_string}".\nError: {soql_err}')

    def describe_fields(self, sobject, print_keys=False):
        try:
            fields = [
                'label', 'name', 'nameField', 'compoundFieldName', 'controllerName', 'custom', 'externalId', 'nillable', 'calculated', 'mask'
            ]

            return self.describe_object(sobject, 'fields', fields, print_keys)
        except Exception as describe_err:
            raise ValueError(
                f'Failed to describe "{sobject}".\nError: {describe_err}')

    def describe_object(self, sobject, key, fields=None, print_keys=False):
        try:
            details = self.session.get(
                self.base_url + '/sobjects/{}/describe'.format(sobject)).json()
            if print_keys:
                print('\ndetail.keys\n', details.keys())
                print('\n'+key+'.keys\n', details[key][0].keys())
            if fields == None:
                fields = details[key][0].keys()

            records = [
                {key: value for key, value in record.items() if key in fields}
                for record in details['fields']
            ]
            return records
        except Exception as describe_err:
            raise ValueError(
                f'Failed to describe "{sobject}".\nError: {describe_err}')

    def get_results(self, url):
        try:
            results = self.session.get(self.instance_url + url)

            return results
        except Exception as describe_err:
            raise ValueError(
                f'Failed to get results.\nError: {describe_err}')

    def close_connection(self):
        self.session.close()
        print('\nClosed connection to Salesforce REST API')
