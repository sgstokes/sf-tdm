#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Imports
import csv
import tools.excel_helper as ex
import tools.helpers as h
import json
import logging
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed


# Logging setup
h.setup_logging(config='./config/logging.json',
                level=logging.DEBUG)

# Logging statements for each module:
log = logging.getLogger(__name__)
log.debug('Logging is configured.')


# Primary function

def create_template(source, object_list, output):
    start_time = h.dtm()
    log.info('create_template starting...')
    log.info(f'source: {source} > output: {output}')

    obj_list = h.get_config(object_list)
    log.debug(f'obj_list: {obj_list}')

    records = get_object_data(source, obj_list.keys())

    template_data = []

    df = pd.DataFrame(records)
    df['relationship'] = df.apply(get_reln_array, axis=1)
    sobjects = df.sobject.unique()

    for obj in sobjects:
        _obj = df[df.sobject == obj]
        _flds = _obj.name.unique()
        _relns = []

        for rel in _obj.relationship.unique():
            if rel != None:
                _rel = rel.split('|')
                _relationships = {
                    'object': _rel[0],
                    'relationshipName': _rel[1],
                    'field': _rel[2],
                    'externalId': obj_list[_rel[0]]['externalId']
                }
                _relns.append(_relationships)

        _template = {
            'type': 'sobject',
            'object': obj,
            'primaryKey': obj_list[obj]['primaryKey'],
            'externalId': obj_list[obj]['externalId'],
            'fields': json.loads(json.dumps(_flds.tolist())),
            'where': '',
            'orderby': '',
            'limit': 0,
            'relationships': _relns,
            'masks': obj_list[obj]['masks']
        }

        template_data.append(_template)

    log.debug(f'template: {template_data}')

    with open(output, 'w') as json_file:
        json.dump(template_data, json_file)

    finish_time = h.dtm()

    return f'create_template completed - run time: {finish_time-start_time}'

# Functions


def get_reln_array(row):
    if row.referenceTo:
        return f'{row.sobject}|{row.referenceTo}|{row.relationshipName}'

    return None


def do_mass_describe(sf_rest, obj_list):
    start_time = h.dtm()
    log.info('do_mass_describe starting')
    results = []

    for obj in obj_list:
        results.extend(sf_rest.describe_fields(obj))

    finish_time = h.dtm()
    log.info(
        f'do_mass_describe completed - run time: {finish_time-start_time}.')

    return results


def get_object_data(source, object_list):
    start_time = h.dtm()
    log.info('get_object_data starting...')

    sf_rest = h.get_sf_rest_connection(source)

    # Mass describe
    results = do_mass_describe(sf_rest, object_list)
    log.info(f'do_mass_describe returned {len(results)} records.')
    # log.debug(records)
    ex.do_excel_out('./output/sttmp.xlsx', results)

    records = []
    for rec in results:
        if rec['updateable'] == True:
            records.append(rec)

    for rec in records:
        if len(rec['referenceTo']) < 1:
            rec['referenceTo'] = None
        else:
            for ref in rec['referenceTo']:
                log.debug(
                    f'for loop {rec["sobject"]}.{rec["name"]} - referenceTo - {ref} in {rec["referenceTo"]}')
                if ref == 'RecordType':
                    rec['referenceTo'] = None
                    rec['relationshipName'] = None
                    break
                if ref == 'User':
                    rec['referenceTo'] = ref
                    break
                if len(rec['referenceTo']) == 1:
                    rec['referenceTo'] = ref
                    break
                rec['referenceTo'] = ref

    # log.debug(records)
    ex.do_excel_out('./output/fntmp.xlsx', records)

    sf_rest.close_connection()
    finish_time = h.dtm()
    log.info(
        f'get_object_data finished - record count: {len(records)} - run time: {finish_time-start_time}.')

    return records


def get_external_id(obj):
    ids = {
        'Account': 'DM_External_ID__c',
        'Client_Transition_DC__c': 'DM_External_ID__c',
        'Contact': 'DM_External_ID__c',
        'Governance_Item__c': 'DM_External_ID__c',
        'Incident__c': 'DM_External_ID__c',
        'Opportunity': 'DM_External_ID__c',
        'Opportunity_Addendum__c': 'DM_External_ID__c',
        'Party_Brand__c': 'DM_External_ID__c',
        'Party_Contact__c': 'DM_External_ID__c',
        'Party_Division__c': 'DM_External_ID__c',
        'Party_Division_Contact__c': 'DM_External_ID__c',
        'Party_Party__c': 'DM_External_ID__c',
        'Plan__c': 'DM_External_ID__c',
        'Plan_Contact__c': 'DM_External_ID__c',
        'Plan_Party__c': 'DM_External_ID__c',
        'Plan_User__c': 'DM_External_ID__c',
        'Plan_Withdrawals__c': 'DM_External_ID__c',
        'Proposed_Plan_Transition__c': 'DM_External_ID__c',
        'Prs_Agreement__c': 'DM_External_ID__c',
        'Prs_Agreement_Contact__c': 'DM_External_ID__c',
        'Prs_Agreement_Instance__c': 'DM_External_ID__c',
        'Prs_Agreement_Version__c': 'DM_External_ID__c',
        'Prs_Business_Data_Endpoint__c': 'DM_External_ID__c',
        'Prs_Communication__c': 'DM_External_ID__c',
        'Prs_Communication_Contact__c': 'DM_External_ID__c',
        'Prs_Communication_Party__c': 'DM_External_ID__c',
        'Prs_Communication_Plan__c': 'DM_External_ID__c',
        'Prs_Communication_Role__c': 'DM_External_ID__c',
        'Prs_Opportunity_Product__c': 'DM_External_ID__c',
        'Prs_Opportunity_Product_Contact__c': 'DM_External_ID__c',
        'Prs_Opportunity_Product_Party__c': 'DM_External_ID__c',
        'Prs_Opportunity_Product_Plan__c': 'DM_External_ID__c',
        'Prs_Opportunity_Product_User__c': 'DM_External_ID__c',
        'Request__c': 'DM_External_ID__c',
        'Role_Group__c': 'DM_External_ID__c',
        'Role_Master__c': 'DM_External_ID__c',
        'Role_Role_Group__c': 'DM_External_ID__c',
        'SaAssignmentDriver__c': 'DM_External_ID__c',
        'SalesAlignment__c': 'DM_External_ID__c',
        'WS_Feature_Flag__c': 'DM_External_ID__c'
    }

    return ids[obj]


# Run main program
if __name__ == '__main__':
    results = create_template(source='./config/prs.prd.json',
                              object_list='./data/sobject.json',
                              output=f'./output/{h.datestamp()}.json')
    log.info(f'{results}\n')
