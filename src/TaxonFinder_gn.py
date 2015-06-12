#!/usr/bin/env python
import requests
import subprocess
import shlex
import math
import sys
import time
import os
import difflib
import json
import fio

def getName(text):
    url = 'http://taxonfinder.org/api/find?text=' + text
    content = requests.get(url)
    data = json.loads(content.text)
    
    if len(data) == 0:
        return ""
    return data[0]['name']

if __name__ == '__main__':
    #print getName('The quick Vulpes jumped over the lazy Canis lupus familiaris')
    #exit(-1)
    
    datadir = "../../good-bad-names-for-GN/data/"
    
    import time
    time_start = time.clock()
    
    
    time_traning = time.clock()
    print "training time: %s" % (time_traning - time_start)
    
    for length in [1, 2, 3]:
        time_start = time.clock()
        
        filename = datadir + 'name_' + str(length) + ".txt"
        
        lines = fio.ReadFile(filename)
        
        goodnames = []
        badnames = []
        partialnames = []
        for i, line in enumerate(lines):
            print i
            line = line.strip()
            result = getName(line)
            name = result
            
            if len(name) == 0:
                badnames.append(line)
                
            elif name != line:
                partialnames.append(line)
            else:
                goodnames.append(line)
        
        fio.SaveList(badnames, datadir + 'badname_length_' + str(length) + "_byTaxonFinder.txt")
        fio.SaveList(partialnames, datadir + 'partialgoodname_length_' + str(length) + "_byTaxonFinder.txt")
        fio.SaveList(goodnames, datadir + 'goodname_length_' + str(length) + "_byTaxonFinder.txt")
            
        time_decoding = time.clock()
        print length, "decoding time: %s" % (time_decoding - time_start)