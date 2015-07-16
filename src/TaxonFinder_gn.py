#!/usr/bin/env python

import math
import sys
import time
import os

import fio

input = "../../good-bad-names-for-GN/data/vertnet_names.txt"
taxonfind_output = "../../good-bad-names-for-GN/data/vertnet_names_taxonfinder_raw.txt"

import time

time_start = time.clock()

lines = fio.ReadFile(input)
lines_taxonfinder = fio.ReadFile(taxonfind_output)
assert(len(lines) == len(lines_taxonfinder))

results = []
for i, line in enumerate(lines):
    line = line.strip()
    name = lines_taxonfinder[i].strip()
    
    if len(name) == 0:
        results.append('No')
    elif name != line:
        results.append('YesNo')
    else:
        results.append('Yes')

fio.SaveList(results, '../../good-bad-names-for-GN/data/vertnet_names_TaxonFinder.txt')
    
time_decoding = time.clock()
print "decoding time: %s" % (time_decoding - time_start)