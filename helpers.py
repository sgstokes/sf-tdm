import json
import time


def get_config(config_file):
    try:
        with open(config_file, 'r') as c:
            config = json.loads(c.read())
            return config
    except Exception as config_err:
        raise ReferenceError(
            f'Failed to load config file "{config_file}": {config_err}')

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
