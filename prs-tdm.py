#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# %% Imports
import csv
import helpers as h
import logging

from concurrent.futures import ThreadPoolExecutor, as_completed


# %% Logging setup
h.setup_logging(level=logging.DEBUG)

# Logging statements for each module:
log = logging.getLogger(__name__)
log.debug('Logging is configured.')

# %% Global variables
MAKE_CHANGES = True


# %% Primary function

def run_template(tdm_config, env_path='./config/', env_config='env.map.json'):
    start_time = h.dtm()
    log.info(f'Started {tdm_config} template run')
    try:
        _tdm_config = h.get_config(tdm_config)
        env_map = h.get_config(env_path+env_config)
        log.info(
            f'Successfully read config files: {tdm_config}')
    except Exception as config_err:
        log.exception(f'Failed to open config files: {config_err}.')

    source = _tdm_config['source']
    target = _tdm_config['target']
    data = _tdm_config['data']
    # Get connections to Salesforce.
    sf_rest_source = h.get_sf_rest_connection(env_path+env_map[source])
    sf_rest_target = h.get_sf_rest_connection(env_path+env_map[target])
    sf_bulk_target = h.get_sf_bulk_connection(env_path+env_map[target])

    try:
        for row in data:
            row_start_time = h.dtm()
            operation = row['operation']
            obj = row['object']
            primaryKey = row['primaryKey']
            externalID = row['externalId']
            fields = row['fields']
            where = row['where']
            orderby = row['orderby']
            limit = row['limit']
            relationships = row['relationships']
            masks = row['masks']

            log.info(f'{operation} -- {source}>>{target} -- {obj} started...')
            # Continue if future operations are specified.
            if operation in ['delete', 'execute', 'upsert']:
                log.info(
                    f'{operation} is a future operation.  Not currently supported.')
                continue
            # Perform delete portion of refresh operation and deleteAll operation.
            if operation in ['refresh', 'deleteAll']:
                delete_data = get_data(sf_rest_target, obj, [primaryKey])
                if delete_data:
                    results = do_bulk_job(
                        sf_bulk_target, 'Delete', obj, delete_data)
                    log.info(results)
            # Perform upsert portion of refresh operation and insert operation.
            if operation in ['refresh', 'insert']:
                # Split relationships into two lists: self and other.
                self_relationships = []
                if relationships:
                    for relationship in relationships:
                        if relationship['object'] == obj:
                            self_relationships.append(relationship)
                            relationships.remove(relationship)
                log.debug(f'Self relationships: {self_relationships}')
                log.debug(f'Other relationships: {relationships}')
                fields_1 = fields
                # Remove self relationship fields from fields list.
                log.debug(
                    f'fields before removing self_relationships: {fields_1}')
                if len(self_relationships) > 0:
                    for self_relationship in self_relationships:
                        fields_1.remove(self_relationship['field'])
                log.debug(
                    f'fields after removing self_relationships: {fields_1}')
                # Replace field with relationship.externalId reference.
                if len(relationships) > 0:
                    fields_1 = replace_field_external_ids(
                        relationships, fields_1)
                    log.debug(
                        f'fields after replacing other relationships: {fields_1}')
                # Get data from source to upsert to target.
                source_data = get_data(sf_rest=sf_rest_source,
                                       obj=obj,
                                       fields=fields_1,
                                       where=where,
                                       orderby=orderby,
                                       limit=limit,
                                       masks=masks)
                # Flatten and remove extraneous fields.
                if len(relationships) > 0:
                    source_data = [h.flatten_dict(record)
                                   for record in source_data]
                    source_data = fix_flattened_fields(
                        relationships, fields_1, source_data)
                # Upsert source data into target, less self relationships.
                if source_data:
                    results = do_bulk_job(sf_bulk_target, 'Upsert', obj,
                                          source_data, externalID)
                    log.info(results)
                # Loop through and upsert self relationships.
                if len(self_relationships) > 0:
                    results = do_self_relationship_upsert(sf_rest=sf_rest_source,
                                                          sf_bulk=sf_bulk_target,
                                                          relationships=self_relationships,
                                                          object_name=obj,
                                                          externalID=externalID,
                                                          where=where,
                                                          orderby=orderby,
                                                          limit=limit)
                    log.info(results)
                # Get record count of target object.
                target_data = get_data(sf_rest_target, obj, [
                    f'count({primaryKey}) Ct'])
                log.debug(f'{obj} final count: {target_data}')
            row_finish_time = h.dtm()
            log.info(
                f'{operation} -- {source}>>{target} -- {obj} finished - run time: {row_finish_time-row_start_time}')

        # Close sf_rest connections.
        sf_rest_source.close_connection()
        sf_rest_target.close_connection()

        finish_time = h.dtm()

        return f'Completed {tdm_config} template run - run time: {finish_time-start_time}.'
    except Exception as template_err:
        log.error(
            f'Failed to run template "{tdm_config}".\nError: {template_err}')
        raise


# %% Functions

def replace_field_external_ids(relationships, fields, separator='.'):
    _fields = fields
    for rel in relationships:
        _fields = replace_item_in_list(source=rel['field'],
                                       target=f'{rel["relationshipName"]}{separator}{rel["externalId"]}',
                                       _list=_fields)

    return _fields


def replace_item_in_list(source, target, _list):
    if source == target:
        return _list
    for fld in _list:
        if fld == source:
            _list.append(target)
            _list.remove(source)

    return _list


def fix_flattened_fields(relationships, fields, data):
    _fields = fields
    log.debug(f'Fields before flattened fix: {_fields}')
    for rel in relationships:
        _fields.append(f'{rel["relationshipName"]}_{rel["externalId"]}')

    for rec in data:
        {rec.pop(_key) for _key in list(rec.keys())
         if _fields and _key not in _fields}

        for rel in relationships:
            reln_underscore_reference = f'{rel["relationshipName"]}_{rel["externalId"]}'
            if reln_underscore_reference in rec:
                rec[f'{rel["relationshipName"]}.{rel["externalId"]}'] = rec.pop(
                    reln_underscore_reference)
    # log.debug(f'Data after flattend fix:\n{data}')
    return data


def do_self_relationship_upsert(sf_rest,
                                sf_bulk,
                                relationships,
                                object_name,
                                externalID,
                                where='',
                                orderby='',
                                limit=0):
    start_time = h.dtm()
    log.info('Start self relationship upsert.')

    for rel in relationships:
        rel_start_time = h.dtm()
        reln_dot_reference = f'{rel["relationshipName"]}.{rel["externalId"]}'
        reln_underscore_reference = f'{rel["relationshipName"]}_{rel["externalId"]}'
        log.info(f'Upserting relationship: {reln_dot_reference}.')

        fields = [externalID, f'{reln_dot_reference}']
        if where:
            _where = f'{where} and {rel["field"]} != null'
        else:
            _where = f'{rel["field"]} != null'

        # Todo Selection between source_data_1 and source_data_2 is different for sample versus population.

        source_data = [h.flatten_dict(record)
                       for record in get_data(sf_rest, object_name, fields, _where, orderby, limit)]
        log.debug(f'Initial record count to upsert: {len(source_data)}')

        if source_data:
            for rec in source_data:
                fields.append(f'{reln_underscore_reference}')
                {rec.pop(_key) for _key in list(rec.keys())
                    if fields and _key not in fields}
                rec[f'{reln_dot_reference}'] = rec.pop(
                    f'{reln_underscore_reference}')
            log.debug(f'Final record count to upsert: {len(source_data)}')

            results = do_bulk_job(sf_bulk, 'Upsert', object_name,
                                  source_data, externalID)
            log.info(results)
        rel_finish_time = h.dtm()
        log.info(
            f'Relationshp: {reln_dot_reference} finished - run time: {rel_finish_time-rel_start_time}.')

    finish_time = h.dtm()

    return f'do_self_relationship_upsert finished - run time: {finish_time-start_time}.'


def get_data(sf_rest, obj, fields, where='', orderby='', limit=0, masks={}):
    query = build_soql(obj, fields, where, orderby, limit)
    _masks = masks

    soql_start_time = h.dtm()
    records = sf_rest.soql_query(query)
    soql_finish_time = h.dtm()
    log.info(
        f'get_data result count: {len(records)} - run time: {soql_finish_time-soql_start_time}.')

    if _masks:
        mask_start_time = h.dtm()
        log.debug('get_data apply masks start.')
        for record in records:
            for field, fake_method in _masks.items():
                record.update({field: str(h.get_fake(fake_method))})
        mask_finish_time = h.dtm()
        log.info(
            f'get_data apply masks finished - run time: {mask_finish_time-mask_start_time}.')

    return records


def build_soql(sobject, fields, where='', orderby='', limit=0):
    _select = 'select ' + ', '.join(fields)
    _from = f' from {sobject}'
    _where = ''
    _orderby = ''
    _limit = ''

    if len(where) > 0:
        _where = ' where ' + where
    if len(orderby) > 0:
        _orderby = ' order by ' + orderby
    if limit > 0:
        _limit = ' limit ' + str(limit)

    q = _select+_from+_where+_orderby+_limit
    log.debug(f'build_soql: {q}')

    return q


def do_bulk_job(sf_bulk, job_type, object_name, data, primary_key=''):
    bulk_start_time = h.dtm()
    # Split records into batches of 5000.
    batches = h.chunk_records(data, 5000)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(do_bulk_job_thread, sf_bulk, job_type,
                                   object_name, batch, primary_key) for batch in batches]

    for future in as_completed(futures):
        log.info(future.result())

    bulk_finish_time = h.dtm()

    return f'do_bulk_job completed - run time: {bulk_finish_time-bulk_start_time}.'


def do_bulk_job_thread(sf_bulk, job_type, object_name, data, primary_key):
    bulk_start_time = h.dtm()

    log.debug(f'do_bulk_job_thread {job_type} on {object_name}.')
    if MAKE_CHANGES:
        if job_type == 'Delete':
            batch_results = sf_bulk.create_and_run_delete_job(
                object_name=object_name, data=data)
        else:
            batch_results = sf_bulk.create_and_run_bulk_job(
                job_type=job_type,
                object_name=object_name,
                primary_key=primary_key,
                data=data)
    else:
        log.debug(f'MAKE_CHANGES set to {MAKE_CHANGES}')
        batch_results = []
        n_success = 0
        n_error = 0

    n_success = 0
    n_error = 0

    for result in batch_results:
        if result.success != 'true':
            n_error += 1
            log.warning(
                f'Record Failed in batch: {result.error}.')
        else:
            n_success += 1
    bulk_finish_time = h.dtm()

    return f'do_bulk_job_threads completed with {n_success} successes and {n_error} failures - run time: {bulk_finish_time-bulk_start_time}.'


# %% Run main program
if __name__ == '__main__':
    MAKE_CHANGES = True
    results = run_template(tdm_config='./template.json')
    log.info(f'{results}\n')
