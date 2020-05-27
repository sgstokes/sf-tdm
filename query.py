#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Stephen Stokes: sstokes[at]relationshipvelocity.com'

import json
import logging

import tools.helpers as h

# Logging statements for each module:
log = logging.getLogger(__name__)
log.debug('Logging is configured.')


# Primary function
@h.timer(log)
def do_query():
    sf_rest = h.get_sf_rest_connection('./config/prs.prd.json')

    query = "select Id, Name, UUID__c from account ORDER BY CreatedDate DESC"
    results = sf_rest.soql_query(query)

    # results = sf_rest.describe_fields('Contact')

    # apexBody = 'new BatchSyncOpportunityReporting()'
    # apexBody = 'new UuidUtils()'
    # url = f'/tooling/executeAnonymous/?anonymousBody=Database.executeBatch({apexBody});'
    # results = sf_rest.get_response(url).json()

    with open('./output/query.json', 'w') as json_file:
        json.dump(results, json_file)
    sf_rest.close_connection()

    return len(results)


# Run main program
if __name__ == '__main__':
    h.setup_logging(level=logging.DEBUG)
    log.info(do_query())
