#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Stephen Stokes: sstokes[at]relationshipvelocity.com'

import csv
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import tools.helpers as h

# Logging statements for each module:
log = logging.getLogger(__name__)
log.debug(f'{__name__} logging is configured.')

# Global variables
MAKE_CHANGES = True


# Primary function
@h.exception(log)
@h.timer(log)
def run_template(tdm_config, env_path='./config/', env_config='env.map.json'):
    conf = h.confirm(
        prompt='Are the target Org email settings correct?', resp=False)
    if conf == False:
        return 'Please correct email settings and return.'

    _tdm_config = h.get_config(tdm_config)
    env_map = h.get_config(env_path+env_config)
    log.info(
        f'Successfully read config files: {tdm_config}')

    source = (_tdm_config['source'] if 'source' in _tdm_config else 'PRD')
    target = _tdm_config['target']
    data = _tdm_config['data']
    sf_cfg_source = env_path+env_map[source]
    sf_cfg_target = env_path+env_map[target]

    for row in data:
        row_start_time = h.dtm()
        operation = row['operation']
        obj = row['object']
        thread = (row['bulk_thread'] if 'bulk_thread' in row else True)
        primary_key = (row['primary_key'] if 'primary_key' in row else 'Id')
        external_id = (row['external_id']
                       if 'external_id' in row else 'UUID__c')
        fields = (row['fields'] if 'fields' in row else [])
        where = (row['where'] if 'where' in row else '')
        orderby = (row['orderby'] if 'orderby' in row else '')
        limit = (row['limit'] if 'limit' in row else '')
        relationships = (row['relationships']
                         if 'relationships' in row else [])
        masks = (row['masks'] if 'masks' in row else {})

        log.info(f'{obj} {operation} -- {source}>>{target} started...')
        # Continue if future operations are specified.
        if operation in ['delete', 'execute']:
            log.info(
                f'{operation} is a future operation.  Not currently supported.')
            continue
        # Perform delete portion of refresh operation and deleteAll operation.
        if operation in ['refresh', 'deleteAll']:
            delete_data = get_data(sf_cfg_target, obj, [primary_key])
            if delete_data:
                do_bulk_job(sf_cfg_target=sf_cfg_target,
                            job_type='Delete',
                            object_name=obj,
                            data=delete_data,
                            thread=thread)
        # Perform upsert portion of refresh operation and insert operation.
        if operation in ['refresh', 'upsert']:
            # Split relationships into two lists: self and other.
            self_relationships = []
            if relationships:
                for relationship in relationships:
                    if relationship['object'] == obj:
                        self_relationships.append(relationship)
                        relationships.remove(relationship)
                        if relationship['field'] in fields:
                            fields.remove(relationship['field'])
            log.debug(f'Self relationships: {self_relationships}')
            log.debug(f'Other relationships: {relationships}')
            log.debug(f'fields after removing self_relationships: {fields}')
            # Upsert without self-relationships.
            do_upsert(sf_cfg_source=sf_cfg_source,
                      sf_cfg_target=sf_cfg_target,
                      relationships=relationships,
                      external_id=external_id,
                      object_name=obj,
                      fields=fields,
                      where=where,
                      orderby=orderby,
                      limit=limit,
                      masks=masks,
                      thread=thread)
            # Upsert self relationships.
            if len(self_relationships) > 0:
                _flds, _where = get_self_reln_fields_where(
                    where, self_relationships, external_id)
                do_upsert(sf_cfg_source=sf_cfg_source,
                          sf_cfg_target=sf_cfg_target,
                          relationships=self_relationships,
                          external_id=external_id,
                          object_name=obj,
                          fields=_flds,
                          where=_where,
                          orderby=orderby,
                          limit=limit,
                          masks=masks,
                          thread=thread)
            # Get record count of target object.
            target_data = get_data(sf_cfg_target, obj, [
                                   f'count({primary_key}) Ct'])
            log.debug(f'{obj} final count: {target_data}')
        row_end_time = h.dtm()
        log.info(
            f'{operation} -- {source}>>{target} -- {obj} completed - run time: {row_end_time-row_start_time}')

    return f'Completed {tdm_config} template run.'


# Functions
def replace_field_external_ids(relationships, fields, separator='.'):
    for rel in relationships:
        fields = replace_item_in_list(source=rel['field'],
                                      target=f'{rel["relationshipName"]}{separator}{rel["external_id"]}',
                                      _list=fields)

    return fields


def replace_item_in_list(source, target, _list):
    if source == target:
        return _list
    for fld in _list:
        if fld == source:
            _list.append(target)
            _list.remove(source)

    return _list


def fix_flattened_fields(relationships, fields, data):
    log.debug(f'Fields before fix_flattened_fields: {fields}')
    for rel in relationships:
        fields.append(f'{rel["relationshipName"]}_{rel["external_id"]}')

    for rec in data:
        {rec.pop(key) for key in list(rec.keys())
         if key not in fields}

        for rel in relationships:
            reln_underscore_reference = f'{rel["relationshipName"]}_{rel["external_id"]}'
            reln_dot_reference = f'{rel["relationshipName"]}.{rel["external_id"]}'
            if reln_underscore_reference in rec:
                rec[f'{rel["relationshipName"]}.{rel["external_id"]}'] = rec.pop(
                    reln_underscore_reference)
            if reln_dot_reference not in rec:
                rec.update({reln_dot_reference: None})
    return data


@h.exception(log)
@h.timer(log)
def do_upsert(sf_cfg_source,
              sf_cfg_target,
              relationships,
              external_id,
              object_name,
              fields,
              where='',
              orderby='',
              limit=0,
              masks='',
              thread=True):
    # Replace field with relationship.external_id reference.
    if len(relationships) > 0:
        fields = replace_field_external_ids(relationships, fields)
        log.debug(
            f'Fields after replacing relationships with external_ids: {fields}')
    # Get data from source to upsert to target.
    source_data = get_data(sf_cfg_source=sf_cfg_source,
                           obj=object_name,
                           fields=fields,
                           where=where,
                           orderby=orderby,
                           limit=limit,
                           masks=masks)
    # Flatten and remove extraneous fields.
    if len(relationships) > 0:
        source_data = [h.flatten_dict(record) for record in source_data]
        source_data = fix_flattened_fields(relationships, fields, source_data)
    # Upsert source data into target.
    if source_data:
        do_bulk_job(sf_cfg_target=sf_cfg_target,
                    job_type='Upsert',
                    object_name=object_name,
                    data=source_data,
                    primary_key=external_id,
                    thread=thread)

    return f'do_upsert completed.'


def get_self_reln_fields_where(where, relationships, external_id):
    _flds = [external_id]
    _where = ([where] if len(where) > 0 else [])
    for rel in relationships:
        _flds.append(f'{rel["relationshipName"]}.{rel["external_id"]}')
        _where.append(f'{rel["field"]} != null')

    return _flds, ' and '.join(_where)


@h.exception(log)
@h.timer(log)
def get_data(sf_cfg_source, obj, fields, where='', orderby='', limit=0, masks={}):
    query = build_soql(obj, fields, where, orderby, limit)
    _masks = masks

    soql_start_time = h.dtm()
    sf_rest = h.get_sf_rest_connection(sf_cfg_source)
    records = sf_rest.soql_query(query)
    sf_rest.close_connection()
    soql_end_time = h.dtm()
    log.info(
        f'get_data result count: {len(records)} - run time: {soql_end_time-soql_start_time}.')

    if _masks:
        mask_start_time = h.dtm()
        log.debug('get_data apply masks start.')
        for record in records:
            for field, fake_method in _masks.items():
                record.update({field: str(h.get_fake(fake_method))})
        mask_end_time = h.dtm()
        log.info(
            f'get_data apply masks completed - run time: {mask_end_time-mask_start_time}.')

    return records


def build_soql(sobject, fields, where='', orderby='', limit=0):
    select = 'select ' + ', '.join(fields)
    _from = f' from {sobject}'
    where = (f' where {where}' if len(where) > 0 else '')
    orderby = (f' order by {orderby}' if len(orderby) > 0 else '')
    limit = (f' limit {str(limit)}' if limit > 0 else '')

    q = select + _from + where + orderby + limit
    log.debug(f'build_soql: {q}')

    return q


@h.exception(log)
@h.timer(log)
def do_bulk_job(sf_cfg_target, job_type, object_name, data, thread=True, primary_key=''):
    # Split records into batches by thread count.
    # min_batch based on Salesforce processing details.
    min_batch = 200
    thread_count = (10 if job_type == 'Delete' else 20)
    thread_count = (thread_count if thread else 1)
    chunk_size = len(data) // thread_count
    chunk_size = min_batch - (chunk_size % min_batch) + chunk_size
    chunk_size = (5000 if chunk_size > 5000 else chunk_size)
    log.debug(f'chunk_size: {chunk_size}')

    batches = h.chunk_records(data, chunk_size)

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [executor.submit(do_bulk_job_thread, sf_cfg_target, job_type,
                                   object_name, batch, primary_key) for batch in batches]

    n_success = 0
    n_error = 0
    for future in as_completed(futures):
        _n_success, _n_error = future.result()
        n_success += _n_success
        n_error += _n_error
        log.info(
            f'do_bulk_job_thread {job_type} completed with {_n_success} successes and {_n_error} failures.')
    log.info(
        f'do_bulk_job {object_name} {job_type} completed with {n_success} successes and {n_error} failures.')

    return f'do_bulk_job completed.'


@h.exception(log)
@h.timer(log)
def do_bulk_job_thread(sf_cfg_target, job_type, object_name, data, primary_key):
    log.debug(
        f'do_bulk_job_thread {job_type} on {object_name} - batch size: {len(data)}')
    n_success = 0
    n_error = 0
    # with open(f'./output/bulk{h.timestamp()}.json', 'w') as json_file:
    #     json.dump(data, json_file)
    # Bypass if global is set to False.
    if MAKE_CHANGES:
        if job_type == 'Delete':
            batch_results = h.get_sf_bulk_connection(sf_cfg_target).create_and_run_delete_job(
                object_name=object_name, data=data)
        else:
            batch_results = h.get_sf_bulk_connection(sf_cfg_target).create_and_run_bulk_job(
                job_type=job_type,
                object_name=object_name,
                primary_key=primary_key,
                data=data)
    else:
        log.debug(f'MAKE_CHANGES set to {MAKE_CHANGES}')
        batch_results = []

    for result in batch_results:
        if result.success != 'true':
            n_error += 1
            log.warning(
                f'Record Failed in batch: {result.error}.')
        else:
            n_success += 1

    return n_success, n_error


# Run main program
if __name__ == '__main__':
    MAKE_CHANGES = True
    h.setup_logging()
    results = run_template(tdm_config='./jobs/template.json')
    log.info(f'{results}\n')
