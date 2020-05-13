#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Stephen Stokes: sstokes[at]relationshipvelocity.com'

import logging
import sys

import sf_tdm as tdm
import tools.helpers as h

# Logging statements for each module:
log = logging.getLogger(__name__)
log.debug(f'{__name__} logging is configured.')


# Primary function
@h.exception(log)
@h.timer(log)
def run_template():
    config = sys.argv[1]
    make_changes = (str2bool(sys.argv[2]) if sys.argv[2] else True)
    target = (sys.argv[3] if sys.argv[3] else None)
    results = tdm.run_template(tdm_config=config, make_changes=make_changes, target=target)
    return results


def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


# Run main program
if __name__ == '__main__':
    h.setup_logging()
    conf = h.confirm(
        prompt='Have RecordTypes been provisioned and UUID_Utils executed?', resp=False)
    if conf == False:
        'Please complete those steps and return.'
    else:
        results = run_template()
        log.info(f'{results}\n')
