#!/usr/bin/env python

import math
import sys
import time
import os
import codecs
import fio

import itertools as it

input = "../../good-bad-names-for-GN/data/all_name_strings.txt"
taxonfind_output = "../../good-bad-names-for-GN/data/all_name_strings.taxon.output"
output = "../../good-bad-names-for-GN/data/all_name_strings.taxon"

import time

time_start = time.clock()

fout = codecs.open(output, 'w', 'utf-8')

results = []
for line, name in it.izip(codecs.open(input, 'r', 'utf-8'), codecs.open(taxonfind_output, 'r', 'utf-8')):
    line = line.strip()
    name = name.strip()
    
    if len(name) == 0:
        fout.write('No\r\n')
    elif name != line:
        fout.write('YesNo\r\n')
    else:
        fout.write('Yes\r\n')

fout.close()
   
time_decoding = time.clock()
print "decoding time: %s" % (time_decoding - time_start)