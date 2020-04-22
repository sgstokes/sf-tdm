#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Stephen Stokes: sstokes[at]relationshipvelocity.com'

import csv
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

import tools.excel_helper as ex
import tools.helpers as h

# Logging setup
h.setup_logging()

# Logging statements for each module:
log = logging.getLogger(__name__)
log.debug('Logging is configured.')


# Primary function
@h.exception(log)
@h.timer(log)
def create_template(source, object_list, output):
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

    return f'create_template completed.'


# Functions
def get_reln_array(row):
    if row.referenceTo:
        return f'{row.sobject}|{row.referenceTo}|{row.relationshipName}'

    return None


@h.exception(log)
@h.timer(log)
def do_mass_describe(sf_rest, obj_list):
    results = []

    for obj in obj_list:
        results.extend(sf_rest.describe_fields(obj))

    log.info('do_mass_describe completed.')
    return results


@h.exception(log)
@h.timer(log)
def get_object_data(source, object_list):
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
    log.info(f'get_object_data finished - record count: {len(records)}.')

    return records


# Run main program
if __name__ == '__main__':
    results = create_template(source='./config/prs.prd.json',
                              object_list='./data/sobject.json',
                              output=f'./output/{h.datestamp()}.json')
    log.info(f'{results}\n')
