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

time_start = time.clock()

filename = datadir + 'vertnet_names.txt'

lines = fio.ReadFile(filename)

results = []
for line in lines:
    line = line.strip()
    result = nn.find_names(line)
    name = result[0]
    
    if len(name) == 0:
        results.append('No')
    elif name != line:
        results.append('YesNo')
    else:
        results.append('Yes')

fio.SaveList(results, datadir + 'vertnet_names_NetiNeti_NB.txt')

time_decoding = time.clock()
print "decoding time: %s" % (time_decoding - time_start)