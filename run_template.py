#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Stephen Stokes: sstokes[at]relationshipvelocity.com'

import sys
import sf_tdm as tdm

# Get script argument
try:
    config = sys.argv[1]
except Exception as argument_error:
    print(f'No input arguments.\nError: {argument_error}')
    raise

# Run template
results = tdm.run_template(tdm_config=config)
print(f'{results}\n')
