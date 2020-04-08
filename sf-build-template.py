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

    records = get_object_data(source, object_list)

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
                    'externalId': ''
                }
                _relns.append(_relationships)

        _template = {
            'type': 'sobject',
            'object': obj,
            'primaryKey': 'Id',
            'externalId': '',
            'fields': json.loads(json.dumps(_flds.tolist())),
            'where': '',
            'orderby': '',
            'limit': 0,
            'relationships': _relns,
            'masks': {}
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


def do_mass_describe(sf_rest, list_file):
    start_time = h.dtm()
    log.info('do_mass_describe starting')
    results = []

    with open(list_file, "r") as ifile:
        for line in ifile:
            if not line:
                break
            sobject = line.strip()
            results.extend(sf_rest.describe_fields(sobject))

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


# Run main program
if __name__ == '__main__':
    results = create_template(source='./config/prs.prd.json',
                              object_list='./data/sobject_list.txt',
                              output=f'./output/{h.datestamp()}.json')
    log.info(f'{results}\n')
