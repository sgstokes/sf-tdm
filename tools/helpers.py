#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Stephen Stokes: sstokes[at]relationshipvelocity.com'

import functools
import json
import logging.config
import os
import time
from datetime import datetime

from faker import Faker

from tools import sf_bulk_api, sf_rest_api

# Variables
fake = Faker()


# Setup logging
def setup_logging(config='./config/logging.json', level=logging.INFO):
    # Setup logging configuration
    path = config
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=level)

    return True


# Return config_file as dictionary.
def get_config(config_file):
    try:
        with open(config_file, 'r') as c:
            config = json.loads(c.read())
            return config
    except Exception as config_err:
        raise ReferenceError(
            f'Failed to load config file "{config_file}": {config_err}')


# Get fake value based on method
def get_fake(method):
    if method[:5] == 'fixed':
        mask = {
            "fixed.company": "Acme Dynamite, Inc.",
            "fixed.date_of_birth": "1970-01-01",
            "fixed.ein": "95-8101756",
            "fixed.email": "joel19@gmail.com",
            "fixed.name": "Valerie Duke",
            "fixed.ssn": "247-03-5127"
        }
    else:
        mask = {
            "fake.company": fake.company(),
            "fake.date_of_birth": fake.date_of_birth(minimum_age=21, maximum_age=115),
            "fake.ein": fake.ein(),
            "fake.email": fake.email(),
            "fake.name": fake.name(),
            "fake.ssn": fake.ssn()
        }

    return mask[method]


# Get reference to sf_rest_api module
def get_sf_rest_connection(config):
    _config = get_config(config)

    sf_rest = sf_rest_api.Connection(
        username=_config['sf_username'],
        password=_config['sf_password']+_config['sf_security_token'],
        grant_type=_config['sf_grant_type'],
        client_id=_config['sf_client_id'],
        client_secret=_config['sf_client_secret'],
        sandbox=_config['sf_sandbox'])

    return sf_rest


# Get reference to sf_bulk_api module
def get_sf_bulk_connection(config):
    _config = get_config(config)

    sf_bulk = sf_bulk_api.Connection(
        username=_config['sf_username'],
        password=_config['sf_password'],
        security_token=_config['sf_security_token'],
        sandbox=_config['sf_sandbox'])

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


# Return formatted timestamp or datestamp
def timestamp():
    return datetime.now().strftime('%H:%M:%S.%f')


def datestamp():
    return datetime.now().strftime('%Y%m%d')


def dtm():
    return datetime.now()


# Confirm prompt
def confirm(prompt=None, resp=False):
    if prompt is None:
        prompt = 'Confirm'
    # Sets default value based on "resp" setting.
    if resp:
        _prompt = f'{prompt} [y] | n: '
    else:
        _prompt = f'{prompt} [n] | y: '

    while True:
        ans = input(_prompt)
        if not ans:
            return resp
        if ans.upper() not in ['Y', 'N']:
            print('please enter y or n.')
            continue
        if ans.upper() == 'Y':
            return True
        if ans.upper() == 'N':
            return False


# Decorators
def timer(log):
    def decorator(func):
        # Print the runtime of the decorated function.
        @functools.wraps(func)
        def wrapper_timer(*args, **kwargs):
            log.info(f'Started {func.__name__!r}.')
            start_time = time.perf_counter()
            value = func(*args, **kwargs)
            end_time = time.perf_counter()
            run_time = end_time - start_time
            log.info(
                f'Finished {func.__name__!r} - run time: {run_time:.4f} secs')
            return value
        return wrapper_timer
    return decorator


def exception(log):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                err = f'There was an exception in  {func.__name__}'
                log.exception(err)
                raise
        return wrapper
    return decorator
