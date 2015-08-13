import file_util as fio
import os
from collections import defaultdict
import json
import codecs
import numpy as np
from get_all_parser_featurs_for_all_names import generator_all_name_strings
from VertNet import VertNetCorpus
import sys

def get_simple_features(parser_output, output):
    vertnet = VertNetCorpus()
    
    f_fun = [vertnet.has_question_mark, vertnet.all_caplitized]
    
    stdout_old = sys.stdout
    sys.stdout = codecs.open(output, 'w', 'utf-8')
    
    name_count = 0
    for name in generator_all_name_strings(parser_output):
        row = []
        for f in f_fun:
            row.append(f(name))
        
        fio.PrintList(row)
        
        name_count += 1
        
        #if name_count > 100: break
    
    sys.stdout = stdout_old
        
if __name__ == '__main__':
    import ConfigParser
    
    config = ConfigParser.RawConfigParser()
    config.read('../config/myconfig.cfg')
    
    datadir = config.get('dir', 'data')
    
    parser_output = os.path.join(datadir, 'parser_info.txt')
    
    output = os.path.join(datadir, 'all_name_strings.simple')
    
    get_simple_features(parser_output, output)
    