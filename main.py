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
        fields = row['fields']
        where = row['where']
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
            source_data = get_data(sf_rest_source, obj,
                                   fields, where, orderby, limit, masks)
            if source_data:
                sf_bulk_target.create_and_run_bulk_job(
                    'Upsert', obj, primaryKey, source_data)

        if operation in ['test']:
            if relationships:
                for relationship in relationships:                    
                    for key, value in relationship.items():
                        print(f'{key}:{value}')

            # target_data = get_data(sf_rest_target, obj, [f'count({primaryKey}) Ct'])
            target_data = 'test'
            print('\n', target_data)

        if operation != 'test':
            target_data = get_data(sf_rest_target, obj, [f'count({primaryKey}) Ct'])
            print(f'\n{target_data} -- {externalID}')

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
    _select = 'select ' + ','.join(fields)
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


# %% Run main
print(main())
