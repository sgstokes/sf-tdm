#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# %% Imports
import helpers as h


# %% Variables


# %% Main


def main():
    sf_rest = h.get_sf_rest_connection('./config/prs.dev200404.json')

    query = "select Id, Name, RecordTypeId, EIN__c from Account where name like 'Zero%'"

    results = sf_rest.soql_query(query)
    sf_rest.close_connection()

    return results


# %% Functions


# %% Run main
print(main())
