#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# %% Imports
import csv
import helpers as h
import logging

# %% Logging setup
h.setup_logging(level=logging.DEBUG)

# Include in each module:
log = logging.getLogger(__name__)
log.debug('Logging is configured.')


# %% Functions


def run_template1():
    log.debug(h.dtm())

    return 'Done'


def run_template(tdm_config, env_path='./config/', env_config='env.map.json'):
    try:
        _tdm_config = h.get_config(tdm_config)
        env_map = h.get_config(env_path+env_config)
        log.info(
            f'Successfully read config files: {tdm_config}')
    except Exception as config_err:
        log.exception(f'Failed to connect to Salesforce: {config_err}.')

    source = _tdm_config['source']
    target = _tdm_config['target']
    data = _tdm_config['data']

    sf_rest_source = h.get_sf_rest_connection(env_path+env_map[source])
    sf_rest_target = h.get_sf_rest_connection(env_path+env_map[target])
    sf_bulk_target = h.get_sf_bulk_connection(env_path+env_map[target])

    for row in data:
        operation = row['operation']
        obj = row['object']
        primaryKey = row['primaryKey']
        externalID = row['externalId']
        fields_0 = row['fields']
        where_0 = row['where']
        orderby = row['orderby']
        limit = row['limit']
        relationships = row['relationships']
        masks = row['masks']

        log.info(f'{operation} -- {source}>>{target} -- {obj}\n')
        if operation in ['delete', 'execute', 'upsert']:
            log.info(
                f'{operation} is a future operation.  Not currently supported.')
            continue

        if operation in ['refresh', 'deleteAll']:
            delete_data = get_data(sf_rest_target, obj, [primaryKey])
            if delete_data:
                do_bulk_job(sf_bulk_target, 'Delete', obj, delete_data)

        if operation in ['refresh', 'insert']:
            self_relationships = []
            if relationships:
                for relationship in relationships:
                    if relationship['object'] == obj:
                        self_relationships.append(relationship)
            log.debug(f'self_relationships: {self_relationships}')
            fields_1 = fields_0
            where_1 = where_0

            if len(self_relationships) > 0:
                for self_relationship in self_relationships:
                    fields_1.remove(self_relationship['field'])
            log.debug(f'fields after removing self_relationships: {fields_1}')

            source_data_1 = get_data(sf_rest_source, obj,
                                     fields_1, where_1, orderby, limit, masks)
            if source_data_1:
                do_bulk_job(sf_bulk_target, 'Upsert', obj,
                            source_data_1, externalID)

            if len(self_relationships) > 0:
                log.debug('Start upsert of self_relationships')
                for self_rel in self_relationships:
                    self_dot_reference = f'{self_rel["relationshipName"]}.{self_rel["externalId"]}'
                    self_underscore_reference = f'{self_rel["relationshipName"]}_{self_rel["externalId"]}'
                    log.info(
                        f'Upserting self relationship {self_dot_reference}')
                    fields_2 = [externalID, f'{self_dot_reference}']

                    externalID_data = []
                    for rec in source_data_1:
                        externalID_data.append(rec[externalID])

                    externalID_data = "('" + \
                        "', '".join(externalID_data) + "')"
                    where_2 = f'{externalID} in {externalID_data} and {self_rel["field"]} != null'

                    source_data_2 = [h.flatten_dict(record)
                                     for record in get_data(sf_rest_source, obj, fields_2, where_2)]
                    log.debug(f'Initial record count to upsert: {len(source_data_2)}')

                    if source_data_2:
                        for rec in source_data_2:
                            fields_2.append(f'{self_underscore_reference}')
                            {rec.pop(_key) for _key in list(rec.keys())
                                if fields_2 and _key not in fields_2}
                            rec[f'{self_dot_reference}'] = rec.pop(
                                f'{self_underscore_reference}')
                        log.debug(f'Final record count to upsert: {len(source_data_2)}')

                        do_bulk_job(sf_bulk_target, 'Upsert', obj,
                                    source_data_2, externalID)

            target_data = get_data(sf_rest_target, obj, [
                                   f'count({primaryKey}) Ct'])
            log.debug(target_data)

    sf_rest_source.close_connection()
    sf_rest_target.close_connection()

    return 'Done'


# %% Functions

def get_data(sf_rest, obj, fields, where='', orderby='', limit=0, masks={}):
    query = build_soql(obj, fields, where, orderby, limit)
    _masks = masks

    records = sf_rest.soql_query(query)

    if _masks:
        for record in records:
            for field, fake_method in _masks.items():
                record.update({field: str(h.get_fake(fake_method))})

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
    log.debug(q)

    return q


def do_bulk_job(sf_bulk, job_type, object_name, data, primary_key=""):
    # Split records into batches of 5000.
    batches = h.chunk_records(data, 5000)

    # Iterate through batches of data, run job, & print results.
    for batch in batches:
        log.debug(f'do_bulk_job {job_type} batch size:{len(batch)}')
        if job_type == 'Delete':
            batch_results = sf_bulk.create_and_run_delete_job(
                object_name=object_name, data=batch)
        else:
            batch_results = sf_bulk.create_and_run_bulk_job(
                job_type=job_type,
                object_name=object_name,
                primary_key=primary_key,
                data=batch)

        n_success = 0
        n_error = 0

    for result in batch_results:
        if result.success != 'true':
            n_error += 1
            log.warning(
                f'Record Failed in Batch {batch}: {result.error}. Record ID: {result.id}.')
        else:
            n_success += 1
    return f'Batch Completed with {n_success} successes and {n_error} failures.'


# %% Run main program

if __name__ == '__main__':
    results = run_template(tdm_config='./template.account-refresh.json')
    log.info(f'{results}\n')
