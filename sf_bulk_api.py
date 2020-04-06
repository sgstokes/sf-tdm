__author__ = 'Andrew Shuler: ashuler[at]relationshipvelocity.com'

import json
import csv
import logging

from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
from salesforce_bulk.util import IteratorBytesIO


class Connection(object):
    def __init__(self, username, password, security_token, sandbox=True):
        """
        :param username:
        :type username: str
        :param password:
        :type password: str
        :param security_token:
        :type security_token: str
        :param sandbox: Whether the Salesforce Instance is Production or Sandbox. Default value is False (Production).
        :type sandbox: bool
        """
        # Logging setup
        self.log = logging.getLogger(__name__)
        self.log.info('Signing into Salesforce.')
        try:
            self.bulk = SalesforceBulk(username=username, password=password,
                                       security_token=security_token, sandbox=sandbox)
            self.log.info(
                f'Successfully connected to Salesforce as "{username}".')
        except Exception as auth_err:
            self.log.exception(
                f'Failed to connect to Salesforce: {auth_err}')
            raise

    def create_and_run_delete_job(self, object_name, data):
        job = self.bulk.create_delete_job(object_name, contentType='CSV')
        # Transform data from list of dictionaries into iterable CSV Content Type,
        # since the salesforce_bulk package provides a sweet class for it.
        csv_iter = CsvDictsAdapter(iter(data))
        # Create a batch with the data and add it to the job.
        batch = self.bulk.post_batch(job, csv_iter)
        # Wait for the batch to complete. Default timeout is 10 minutes.
        self.bulk.wait_for_batch(job, batch, timeout=60 * 10)
        # Once the batch has been completed, get the results.
        results = self.bulk.get_batch_results(batch)
        # Close the Job.
        self.bulk.close_job(job)
        self.log.info(
            f'Delete {object_name} job has been successfully completed.')
        return results

    def create_and_run_bulk_job(self, job_type, object_name, primary_key, data):
        """
        Note: If the specified job_type is Update or Upsert, you must provide the kwarg "primary_key" for
        Salesforce to identify records with.

        :param job_type: "Update", "Insert", or "Upsert".
        :type job_type: str
        :param object_name: The Name of the SF Object you are performing a bulk job on.
                            Ex. object_name = "Account"
        :type object_name: str
        :param primary_key:
        :type primary_key: str
        :param data: Needs to be formatted as a list of dictionaries.
                     Ex. data = [{'Id': 1, 'Name': 'Andrew'}, {'Id': 2, 'Name': 'Jerry'}]
        :type data: list
        """
        # Ensure string arguments have their first letter capitalized.
        job_type = str.title(job_type)
        object_name = str.title(object_name)
        # Connect and authenticate to Salesforce.
        # Create Job.
        self.log.info(f'Creating {object_name} {job_type} job.')
        if job_type not in ['Insert', 'Update', 'Upsert']:
            raise ReferenceError(
                'Invalid job_type not specified. Please use "Insert", "Update", or "Upsert".')
        try:
            if job_type == 'Insert':
                job = self.bulk.create_insert_job(
                    object_name, contentType='CSV')
            elif job_type == 'Update':
                job = self.bulk.create_update_job(
                    object_name, contentType='CSV')
                soql_query = f'select Id, {primary_key} from {object_name}'
                query_job = self.bulk.create_query_job(
                    object_name, contentType='CSV')
                query_batch = self.bulk.query(query_job, soql_query)
                self.bulk.close_job(query_job)
                self.bulk.wait_for_batch(query_job, query_batch, timeout=60*10)
                query_results = list(
                    self.bulk.get_all_results_for_query_batch(query_batch))
                if len(query_results) == 1:
                    id_map = json.load(IteratorBytesIO(query_results[0]))
                    for rec in data:
                        for row in id_map:
                            if primary_key not in row:
                                key_split = primary_key.split('.')
                                row[primary_key] = row[key_split[0]][key_split[1]]
                            if rec[primary_key] == row[primary_key]:
                                rec['Id'] = row['Id']
                                break
                else:
                    raise OverflowError(
                        'Query Results larger than expected. Please review.')
            elif job_type == 'Upsert':
                job = self.bulk.create_upsert_job(
                    object_name, external_id_name=primary_key, contentType='CSV')
        except Exception as job_creation_error:
            self.log.info(
                f'Unable to create {object_name} {job_type} Job. Please verify the value of the object_name variable.')
            self.log.exception(
                f'Encountered exception when creating job: {job_creation_error}')
            raise

        # Transform data from list of dictionaries into iterable CSV Content Type,
        # since the salesforce_bulk package provides a sweet class for it.
        csv_iter = CsvDictsAdapter(iter(data))
        # Create a batch with the data and add it to the job.
        batch = self.bulk.post_batch(job, csv_iter)
        # Wait for the batch to complete. Default timeout is 10 minutes.
        self.bulk.wait_for_batch(job, batch, timeout=60*10)
        # Once the batch has been completed, get the results.
        results = self.bulk.get_batch_results(batch)
        # Close the Job.
        self.bulk.close_job(job)
        self.log.info(
            f'{job_type}, {object_name}, job has been successfully completed.')
        return results
