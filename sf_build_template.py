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
def create_template(source_org, operations_list, output, ext_id_file, fields=None):
    log.info(f'source: {source_org} > output: {output}')

    ext_id = h.get_config(ext_id_file)
    log.debug(f'ext_id: {ext_id}')

    opr_list = h.get_config(operations_list)
    log.debug(f'operations_list: {opr_list}')

    obj_list = list(dict.fromkeys([d['object']
                                   for d in opr_list['operations']]))

    all_fields = get_object_data(source_org, obj_list, fields)

    fields = []
    for fld in all_fields:
        if (not fld['referenceTo']) or (fld['referenceTo'] and fld['relationshipName'] and fld['referenceTo'] in ext_id):
            fields.append(fld)

    template_data = []
    for rec in opr_list['operations']:
        _obj = rec['object']
        _flds = (rec['fields'] if 'fields' in rec else [f['name']
                                                        for f in fields if f['sobject'] == _obj])
        _relns = [
            {
                'object': f['referenceTo'],
                'relationship_name': f['relationshipName'],
                'field': f['name'],
                'external_id': (ext_id[f['referenceTo']] if f['referenceTo'] in ext_id else 'UUID__c')
            }
            for f in fields
            if f['sobject'] == _obj and f['referenceTo'] != None]

        _template = {
            'operation': rec['operation'],
            'object': _obj,
            'bulk_thread': (rec['bulk_thread'] if 'bulk_thread' in rec else True),
            'primary_key': 'Id',
            'external_id': (ext_id[_obj] if _obj in ext_id else 'UUID__c'),
            'fields': _flds,
            'where': '',
            'order_by': '',
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
def get_object_data(source_org, obj_list, fields=None):
    sf_rest = h.get_sf_rest_connection(source_org)

    # Mass describe
    if fields:
        all_fields = h.get_config(fields)
    else:
        for obj in obj_list:
            all_fields.extend(sf_rest.describe_fields(obj))

    # log.debug(all_fields)
    log.info(f'do_mass_describe returned {len(all_fields)} records.')
    with open('./output/fields_0.json', 'w') as json_file:
        json.dump(all_fields, json_file)

    fields = []
    for fld in all_fields:
        # TODO Verify filter.
        if fld['createable']:
            fields.append(fld)

    for fld in fields:
        ref_to = fld['referenceTo']
        if (len(ref_to) > 1) or ('RecordType' in ref_to):
            fld['referenceTo'] = None
            fld['relationshipName'] = None
        else:
            fld['referenceTo'] = (None if len(ref_to) < 1 else ''.join(ref_to))

    # log.debug(fields)
    with open('./output/fields_1.json', 'w') as json_file:
        json.dump(fields, json_file)

    sf_rest.close_connection()
    log.info(f'get_object_data finished - record count: {len(fields)}.')

    return fields


# Run main program
if __name__ == '__main__':
    h.setup_logging()
    results = create_template(source_org='./config/prs.prd.json',
                              operations_list='./data/operations-list.json',
                              output=f'./output/{h.datestamp()}.json',
                              ext_id_file='./data/ext-id.json',
                              fields='./data/fields_0.json')
    log.info(f'{results}\n')
