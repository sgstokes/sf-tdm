#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# %% Imports
import helpers as h


# %% Variables


# %% Main


def main():
    sf_rest = h.get_sf_rest_connection('./config/prs.dev200404.json')

    query = "select Id, Name, RecordType.Name, ParentId, Parent.Legacy_ID__c, EIN__c from Account"
    query = "select Id, Name, SobjectType from RecordType where SobjectType = 'Account'"
    query = "select Id, Id from Account"
    #  where Legacy_ID__c in ('0013900001PP4E7AAL', '00170000010p2tdAAA')"
    # query = "select Id, Name, ParentId, EIN__c from Account where Parent in ('0013900001PP4E7AAL', '00170000010p2tdAAA')"

    results = sf_rest.soql_query(query)
    sf_rest.close_connection()

    return results


# %% Functions


# %% Run main
print(main())
