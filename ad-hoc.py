#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# %% Imports
import csv
import helpers as h
import sf_rest_api
import sf_bulk_api
import os
import pandas as pd


# %% Variables
config = './config/prs.stg.json'
fn_path = os.path.dirname(os.path.realpath(__file__))

# %% Main


def main():
    results = do_query()
    # results = do_bulk()
    # results = do_bulk_delete()

    print(results)

    return results


# %% Queries


def do_query():
    query = ('select '
             'Id, Name, RecordType.Name, copado__Business_Analyst__r.Name, copado__Developer__r.Name, '
             'copado__Epic__r.copado__Epic_Title__c, copado__userStory_Role__c, copado__userStory_need__c, '
             'copado__userStory_reason__c, copado__Functional_Specifications__c, copado__Priority__c, '
             'copado__Release__r.Name, copado__Sprint__r.Name, copado__Status__c, '
             'copado__Story_Points_SFDC__c '
             'from copado__User_Story__c')

    results = sf_rest.soql_query(query)

    return results


# %% Bulk


def do_bulk():
    input_file = './data/role.csv'
    _job_type = 'INSERT'  # INSERT, UPSERT, UPDATE, DELETE
    _object_name = 'Role_Master__c'
    # YOU CAN LEAVE THIS FIELD EMPTY FOR INSERT OPERATIONS.
    _primary_key = ''

    # Load data from CSV into dictionary and clean values.
    with open(input_file, 'r', newline='') as i:
        input_data = [line for line in csv.reader(i, delimiter=',')]
        fields = input_data.pop(0)
    fmt_data = [dict(zip(fields, row)) for row in input_data]
    for record in fmt_data:
        for key, val in record.items():
            # Convert any None-type values to empty string.
            if val is None:
                record[key] = ''
            # Convert any non-strings values into strings.
            elif not isinstance(val, str):
                record[key] = str(val)

    # Split records into batches of 5000.
    batches = h.chunk_records(fmt_data, 5000)

    # Iterate through batches of data, run job, & print results.
    for batch in batches:
        batch_results = sf_bulk.create_and_run_bulk_job(
            job_type=_job_type,
            object_name=_object_name,
            primary_key=_primary_key,
            data=batch)
        n_success = 0
        n_error = 0
        for result in batch_results:
            if result.success != 'true':
                n_error += 1
                print(
                    f'Record Failed in Batch {batch}: {result.error}. Record ID: {result.id}.')
            else:
                n_success += 1
        return f'Batch Completed with {n_success} successes and {n_error} failures.'


def do_bulk_delete():
    query = 'select Id from Role_Master__c'

    id_list = sf_rest.soql_query(query)

    bulk_results = sf_bulk.create_and_run_delete_job('Role_Master__c', id_list)

    return bulk_results


# %% Connection

def get_sf_rest_connection(config):
    config_complete = os.path.join(fn_path, config)
    config = h.get_config(config_complete)

    sf_rest = sf_rest_api.Connection(
        username=config['sf_username'],
        password=config['sf_password']+config['sf_security_token'],
        grant_type=config['sf_grant_type'],
        client_id=config['sf_client_id'],
        client_secret=config['sf_client_secret'],
        sandbox=config['sf_sandbox'])

    return sf_rest


def get_sf_bulk_connection(config):
    config_complete = os.path.join(fn_path, config)
    config = h.get_config(config_complete)

    sf_bulk = sf_bulk_api.Connection(
        username=config['sf_username'],
        password=config['sf_password'],
        security_token=config['sf_security_token'],
        sandbox=config['sf_sandbox'])

    return sf_bulk

# sf_rest_prd = get_sf_rest_connection('./config/prs.prd.json')
# sf_bulk_prd = get_sf_bulk_connection('./config/prs.prd.json')


sf_rest = get_sf_rest_connection('./config/prs.prd.json')
sf_bulk = get_sf_bulk_connection('./config/prs.prd.json')


# %% Run main

print(main())

# %% Close Salesforce connection
sf_rest.close_connection()
