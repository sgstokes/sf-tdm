#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Stephen Stokes: sstokes[at]relationshipvelocity.com'

# Imports
import tools.helpers as h


# Code
st = h.dtm()
for x in range(1000):
    method = 'fake.ein'
    str(h.get_fake(method))
fn = h.dtm()
print(f'{method}: {fn-st}')

st = h.dtm()
for x in range(1000):
    method = 'fixed.ein'
    str(h.get_fake(method))
fn = h.dtm()
print(f'{method}: {fn-st}')
