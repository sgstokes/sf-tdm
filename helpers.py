#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# %% Imports

import json
import os
import sf_rest_api
import sf_bulk_api
import time

fn_path = os.path.dirname(os.path.realpath(__file__))


def get_config(config_file):
    try:
        with open(config_file, 'r') as c:
            config = json.loads(c.read())
            return config
    except Exception as config_err:
        raise ReferenceError(
            f'Failed to load config file "{config_file}": {config_err}')


def get_sf_rest_connection(config):
    config_complete = os.path.join(fn_path, config)
    config = get_config(config_complete)

    sf_rest = sf_rest_api.Connection(
        username=config['sf_username'],
        password=config['sf_password']+config['sf_security_token'],
        grant_type=config['sf_grant_type'],
        client_id=config['sf_client_id'],
        client_secret=config['sf_client_secret'],
        sandbox=config['sf_sandbox'])

    return sf_rest


def get_sf_bulk_connection(config):
    config_complete = os.path.join(fn_path, config)
    config = get_config(config_complete)

    sf_bulk = sf_bulk_api.Connection(
        username=config['sf_username'],
        password=config['sf_password'],
        security_token=config['sf_security_token'],
        sandbox=config['sf_sandbox'])

    return sf_bulk

# Conversion of nested dictionary into flattened dictionary


def flatten_dict(dd, separator='_', prefix=''):
    return {prefix + separator + k if prefix else k: v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
            } if isinstance(dd, dict) else {prefix: dd}

# Create chunks for Bulk


def chunk_records(records, chunk_size):
    for step in range(0, len(records), chunk_size):
        yield records[step:step+chunk_size]

# Return formatted timestamp


def timestamp():
    return time.strftime('%H:%M:%S.%f')
