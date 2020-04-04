#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# %% Imports
import csv
import helpers as h


# %% Variables
env_path = './config/'
env_config = 'env.map.json'
tdm_config = './template.account-refresh.json'

# %% Main


def main():
    _tdm_config = h.get_config(tdm_config)
    env_map = h.get_config(env_path+env_config)

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

        print('\n', '*'*80, f'\n{operation} -- {source}>>{target} -- {obj}\n')
        if operation in ['execute', 'upsert']:
            print(f'{operation} is a future operation.  Not currently supported.')
            continue

        if operation in ['refresh', 'deleteAll']:
            delete_data = get_data(sf_rest_target, obj, [primaryKey])
            if delete_data:
                sf_bulk_target.create_and_run_delete_job(obj, delete_data)

        if operation in ['refresh', 'insert']:
            if relationships:
                for relationship in relationships:
                    if relationship['object'] == obj:
                        self_field = relationship['field']
                        self_relationshipName = relationship['relationshipName']
                        self_externalId = relationship['externalId']

            fields_1 = fields_0
            where_1 = where_0
            fields_1.pop(fields_1.index(self_field))

            source_data_1 = get_data(sf_rest_source, obj,
                                     fields_1, where_1, orderby, limit, masks)
            if source_data_1:
                do_bulk(sf_bulk_target, 'Upsert', obj,
                        externalID, source_data_1)

            fields_2 = [externalID,
                        f'{self_relationshipName}.{self_externalId}']

            externalID_data = []
            for rec in source_data_1:
                externalID_data.append(rec[externalID])

            externalID_data = "('" + "', '".join(externalID_data) + "')"
            where_2 = f'{externalID} in {externalID_data}'

            source_data_2 = get_data(sf_rest_source, obj, fields_2, where_2)
            source_data_2 = [h.flatten_dict(record)
                             for record in source_data_2]
            parent_count = 0
            for rec in source_data_2:
                if rec.get(self_relationshipName, -1) != -1:
                    source_data_2.remove(rec)
            for rec in source_data_2:
                parent_count += 1
                fields_2.append(
                    f'{self_relationshipName}_{self_externalId}')
                {rec.pop(_key) for _key in list(rec.keys())
                    if fields_2 and _key not in fields_2}
                rec[f'{self_relationshipName}.{self_externalId}'] = rec.pop(
                    f'{self_relationshipName}_{self_externalId}')

            print(source_data_2)
            if source_data_2 and parent_count > 0:
                do_bulk(sf_bulk_target, 'Upsert', obj,
                        externalID, source_data_2)

            target_data = get_data(sf_rest_target, obj, [
                                   f'count({primaryKey}) Ct'])
            print('\n', target_data)

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

    return _select+_from+_where+_orderby+_limit


def do_bulk(sf_bulk, job_type, object_name, primary_key, data):
    # Split records into batches of 5000.
    batches = h.chunk_records(data, 5000)

    # Iterate through batches of data, run job, & print results.
    for batch in batches:
        print(len(batch))
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
            print(
                f'Record Failed in Batch {batch}: {result.error}. Record ID: {result.id}.')
        else:
            n_success += 1
    return f'Batch Completed with {n_success} successes and {n_error} failures.'


# %% Run main
print(main())
