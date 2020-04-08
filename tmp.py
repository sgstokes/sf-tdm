#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Imports
import tools.helpers as h

from faker import Faker


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
