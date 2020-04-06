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

            log.info(f'{operation} -- {source}>>{target} -- {obj}\n')
            # Continue if future operations are specified.
            if operation in ['delete', 'execute', 'upsert']:
                log.info(
                    f'{operation} is a future operation.  Not currently supported.')
                continue
            # Perform delete portion of refresh operation and deleteAll operation.
            if operation in ['refresh', 'deleteAll']:
                delete_data = get_data(sf_rest_target, obj, [primaryKey])
                if delete_data:
                    do_bulk_job(sf_bulk_target, 'Delete', obj, delete_data)
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
                if len(self_relationships) > 0:
                    for self_relationship in self_relationships:
                        fields_1.remove(self_relationship['field'])
                log.debug(
                    f'fields after removing self_relationships: {fields_1}')
                # Todo Deal with other relationships.**************************
                # Get data from source to upsert to target.
                source_data = get_data(sf_rest_source, obj,
                                       fields_1, where, orderby, limit, masks)
                # Upsert source data into target, less self relationships.
                if source_data:
                    do_bulk_job(sf_bulk_target, 'Upsert', obj,
                                source_data, externalID)
                # Loop through and upsert self relationships.
                if len(self_relationships) > 0:
                    for rel in self_relationships:
                        results = do_relationship_upsert(sf_rest=sf_rest_source,
                                                         sf_bulk=sf_bulk_target,
                                                         relationship=rel,
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
        # Close sf_rest connections.
        sf_rest_source.close_connection()
        sf_rest_target.close_connection()

        return f'Completed {tdm_config} template run.'
    except Exception as template_err:
        log.error(
            f'Failed to run template "{tdm_config}".\nError: {template_err}')
        raise


# %% Functions

def do_relationship_upsert(sf_rest, sf_bulk, relationship, object_name, externalID, where='', orderby='', limit=0):
    log.info('Start relationship upsert')

    if len(relationship) > 0:
        reln_dot_reference = f'{relationship["relationshipName"]}.{relationship["externalId"]}'
        reln_underscore_reference = f'{relationship["relationshipName"]}_{relationship["externalId"]}'
        log.info(f'Upserting relationship {reln_dot_reference}')

        fields = [externalID, f'{reln_dot_reference}']
        if where:
            _where = f'{where} and {relationship["field"]} != null'
        else:
            _where = f'{relationship["field"]} != null'

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

            do_bulk_job(sf_bulk, 'Upsert', object_name,
                        source_data, externalID)

    return f'Relationship upsert completed.'


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


def do_bulk_job(sf_bulk, job_type, object_name, data, primary_key=''):
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
    results = run_template(tdm_config='./template.json')
    log.info(f'{results}\n')
