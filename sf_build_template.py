#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Stephen Stokes: sstokes[at]relationshipvelocity.com'

import json
import logging

import tools.helpers as h

# Logging statements for each module:
log = logging.getLogger(__name__)
log.debug(f'{__name__} logging is configured.')


# Primary function
@h.exception(log)
@h.timer(log)
def create_template(source, operations_list, output, fields=None):
    log.info(f'source: {source} > output: {output}')

    ext_id = h.get_config('./data/ext-id.json')
    log.debug(f'ext_id: {ext_id}')

    opr_list = h.get_config(operations_list)
    log.debug(f'operations_list: {opr_list}')

    obj_list = list(dict.fromkeys([d['object']
                                   for d in opr_list['operations']]))

    all_fields = get_object_data(source, obj_list, fields)
    fields = []

    for fld in all_fields:
        if (not fld['referenceTo']) or (fld['referenceTo'] and fld['relationshipName'] and fld['referenceTo'] in ext_id):
            fields.append(fld)

    template_data = []

    for rec in opr_list['operations']:
        _obj = rec['object']
        _flds = (rec['fields'] if 'fields' in rec else [f['name'] for f in fields if f['sobject'] == _obj])
        _relns = [{'object': f['referenceTo'],
                   'relationshipName': f['relationshipName'],
                   'field': f['name'],
                   'externalId': ext_id[f['referenceTo']]}
                  for f in fields
                  if f['sobject'] == _obj and f['referenceTo'] != None]

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

    # log.debug(f'template: {template_data}')

    with open(output, 'w') as json_file:
        json.dump(template_data, json_file)

    return f'create_template completed.'


# Functions
@h.exception(log)
@h.timer(log)
def get_object_data(source, obj_list, fields=None):
    sf_rest = h.get_sf_rest_connection(source)

    # Mass describe
    all_fields = (h.get_config(fields) if fields else [sf_rest.describe_fields(obj)[0] for obj in obj_list])
    # log.debug(all_fields)
    log.info(f'do_mass_describe returned {len(all_fields)} records.')
    with open('./output/sttmp.json', 'w') as json_file:
        json.dump(all_fields, json_file)

    fields = []
    for fld in all_fields:
        if fld['updateable'] == True or fld['nillable'] == False:
            fields.append(fld)

    for fld in fields:
        ref_to = fld['referenceTo']
        if (len(ref_to) > 1) or ('RecordType' in ref_to):
            fld['referenceTo'] = None
            fld['relationshipName'] = None
        else:
            fld['referenceTo'] = (None if len(ref_to) < 1 else ''.join(ref_to))

    # log.debug(fields)
    with open('./output/fntmp.json', 'w') as json_file:
        json.dump(fields, json_file)

    sf_rest.close_connection()
    log.info(f'get_object_data finished - record count: {len(fields)}.')

    return fields


# Run main program
if __name__ == '__main__':
    h.setup_logging()
    results = create_template(source='./config/prs.prd.json',
                              operations_list='./data/operations-list.json',
                              output=f'./output/{h.datestamp()}.json',
                              fields='./data/fields.json')
    log.info(f'{results}\n')
