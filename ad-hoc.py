#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# %% Imports
import csv
import helpers as h
import os


# %% Variables
env_path = './config/'
env_config = 'env.map.json'
tdm_config = './template.json'

# %% Main



def main():
    _tdm_config = h.get_config(tdm_config)
    env_map = h.get_config(env_path+env_config)

    source = _tdm_config['source']
    target = _tdm_config['target']
    data = _tdm_config['data']

    sf_rest_source = h.get_sf_rest_connection(env_path+env_map[source])
    sf_rest_target = h.get_sf_rest_connection(env_path+env_map[target])

    for row in data:
        print('\n', '*'*80, '\n', row['operation']+'-'+row['object'], '\n')
        if row['operation'] in ['execute', 'refresh']:
            print(
                f'{row["operation"]} is a future operation.  Not currently supported.')
            continue

        source_data = get_data(sf_rest_source, row)
        target_data = get_data(sf_rest_target, row)

        print('\n', source_data)
        print('\n', target_data)

    sf_rest_source.close_connection()
    sf_rest_target.close_connection()

    return 'Done'


# %% Functions

def get_data(sf_rest, row):
    query = build_soql(row['object'], row['fields'],
                       row['where'], row['orderby'], row['limit'])
    masks = row['masks']

    records = sf_rest.soql_query(query)

    if masks:
        for record in records:
            for field, fake_method in masks.items():
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
