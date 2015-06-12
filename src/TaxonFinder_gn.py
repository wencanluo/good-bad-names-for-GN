#!/usr/bin/env python

import math
import sys
import time
import os

import fio

datadir = "../../good-bad-names-for-GN/data/"
taxonfind_dir = "../../node-taxonfinder/"

import time

for length in [1, 2, 3]:
    time_start = time.clock()
    
    filename = datadir + 'name_' + str(length) + ".txt"
    taxonfinder_names = taxonfind_dir + 'name_' + str(length) + ".txt"
    
    lines = fio.ReadFile(filename)
    lines_taxonfinder = fio.ReadFile(taxonfinder_names)
    assert(len(lines) == len(lines_taxonfinder))
    
    goodnames = []
    badnames = []
    partialnames = []
    for i, line in enumerate(lines):
        line = line.strip()
        name = lines_taxonfinder[i].strip()
        
        if len(name) == 0:
            badnames.append(line)
            
        elif name != line:
            partialnames.append(line)
        else:
            goodnames.append(line)
    
    fio.SaveList(badnames, datadir + 'badname_length_' + str(length) + "_byTaxonFiner.txt")
    fio.SaveList(partialnames, datadir + 'partialgoodname_length_' + str(length) + "_byTaxonFiner.txt")
    fio.SaveList(goodnames, datadir + 'goodname_length_' + str(length) + "_byTaxonFiner.txt")
        
    time_decoding = time.clock()
    print length, "decoding time: %s" % (time_decoding - time_start)