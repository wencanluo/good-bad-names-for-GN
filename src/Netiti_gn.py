#!/usr/bin/env python
from neti_neti_trainer import NetiNetiTrainer
from neti_neti import NetiNeti
import subprocess
import shlex
import math
import sys
import time
import os
import difflib

import fio

datadir = "../../good-bad-names-for-GN/data/"

import time
time_start = time.clock()

import pickle
classifier = NetiNetiTrainer(learning_algorithm='SVM')
with open('classifier_SVM.pickle', 'wb') as handle:
    pickle.dump(classifier, handle)
# exit(-1)

# with open('classifier.pickle', 'rb') as handle:
#      classifier = pickle.load(handle)

nn = NetiNeti(classifier)

time_traning = time.clock()
print "training time: %s" % (time_traning - time_start)

for length in [1, 2, 3]:
    time_start = time.clock()
    
    filename = datadir + 'name_' + str(length) + ".txt"
    
    lines = fio.ReadFile(filename)
    
    goodnames = []
    badnames = []
    partialnames = []
    for line in lines:
        line = line.strip()
        result = nn.find_names(line)
        name = result[0]
        
        if len(name) == 0:
            badnames.append(line)
            
        elif name != line:
            partialnames.append(line)
        else:
            goodnames.append(line)
    
    fio.SaveList(badnames, datadir + 'badname_length_' + str(length) + "_bySVM.txt")
    fio.SaveList(partialnames, datadir + 'partialgoodname_length_' + str(length) + "_bySVM.txt")
    fio.SaveList(goodnames, datadir + 'goodname_length_' + str(length) + "_bySVM.txt")
        
    time_decoding = time.clock()
    print length, "decoding time: %s" % (time_decoding - time_start)