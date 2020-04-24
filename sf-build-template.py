#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Stephen Stokes: sstokes[at]relationshipvelocity.com'

import json
import logging

import tools.helpers as h

# Logging setup
h.setup_logging()

# Logging statements for each module:
log = logging.getLogger(__name__)
log.debug('Logging is configured.')


# Primary function
@h.exception(log)
@h.timer(log)
def create_template(source, operations_list, output):
    log.info(f'source: {source} > output: {output}')

    ext_id = h.get_config('./data/ext-id.json')
    log.debug(f'ext_id: {ext_id}')

    opr_list = h.get_config(operations_list)
    log.debug(f'operations_list: {opr_list}')

    obj_list = list(dict.fromkeys([d['object']
                                   for d in opr_list['operations']]))

    records = get_object_data(source, obj_list)
    details = []

    for rec in records:
        if (not rec['referenceTo']) or (rec['referenceTo'] and rec['relationshipName'] and rec['referenceTo'] in ext_id):
            details.append(rec)

    template_data = []

    for rec in opr_list['operations']:
        _obj = rec['object']
        _flds = [d['name'] for d in details if d['sobject'] == _obj]
        _relns = [{'object': d['referenceTo'],
                   'relationshipName': d['relationshipName'],
                   'field': d['name'],
                   'externalId': ext_id[d['referenceTo']]}
                  for d in details
                  if d['sobject'] == _obj and d['referenceTo'] != None]

        _template = {
            'operation': rec['operation'],
            'type': 'sobject',
            'object': _obj,
            'primaryKey': 'Id',
            'externalId': ext_id[_obj],
            'fields': _flds,
            'where': '',
            'orderby': '',
            'limit': 0,
            'relationships': _relns,
            'masks': rec['masks']
        }

        template_data.append(_template)

    log.debug(f'template: {template_data}')

    with open(output, 'w') as json_file:
        json.dump(template_data, json_file)

    return f'create_template completed.'


# Functions
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
def get_object_data(source, obj_list):
    sf_rest = h.get_sf_rest_connection(source)

    # Mass describe
    results = do_mass_describe(sf_rest, obj_list)
    log.info(f'do_mass_describe returned {len(results)} records.')
    # log.debug(records)
    with open('./output/sttmp.json', 'w') as json_file:
        json.dump(results, json_file)

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
    with open('./output/fntmp.json', 'w') as json_file:
        json.dump(records, json_file)

    sf_rest.close_connection()
    log.info(f'get_object_data finished - record count: {len(records)}.')

    return records


# Run main program
if __name__ == '__main__':
    results = create_template(source='./config/prs.prd.json',
                              operations_list='./data/operations-list.json',
                              output=f'./output/{h.datestamp()}.json')
    log.info(f'{results}\n')
