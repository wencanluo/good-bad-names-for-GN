from collections import defaultdict
import fio
import json
import matplotlib.pyplot as plt
import numpy as np

def plot_data_source_distribution(distribution_file, data_sources):
    dis_dict = fio.LoadDict(distribution_file, 'int')
    data_source_dict = fio.LoadDict(data_sources, 'str')
    
    #keys = dis_dict.keys()
    keys = sorted(dis_dict, key=dis_dict.get, reverse=True)
    
    #XX = [data_source_dict[key] for key in keys]
    X = [key for key in keys]
    Y = [dis_dict[key] for key in keys]
    
    head = ['data source id', 'title', 'number of names']
    body = []
    for x,y in zip(X, Y):
        row = [x, data_source_dict[x], y]
        body.append(row)
    fio.WriteMatrix(distribution_file + 'name', body, head)
    
    
def get_data_source_distribution(combine_canonical_form_with_sources, output):
    dict = defaultdict(int)
    for line in open(combine_canonical_form_with_sources):
        tokens = line.rstrip('\r\n').split('\t')
        
        sources = tokens[1].split(',')
        
        for source in sources:
            dict[source] += 1
    
    fio.SaveDict(dict, output, True)
        
if __name__ == '__main__':
    import ConfigParser
    
    config = ConfigParser.RawConfigParser()
    config.read('../config/myconfig.cfg')
    
    datadir = config.get('dir', 'data')
    
    import os
    combine_canonical_form_with_sources = os.path.join(datadir, 'canonical_forms_id_with_sources.txt')
    data_source_distribution = os.path.join(datadir, 'data_source_distribution.txt')
    
    #get_data_source_distribution(combine_canonical_form_with_sources, data_source_distribution)
    data_sources = os.path.join(datadir,'data_sources.dict')
    plot_data_source_distribution(data_source_distribution, data_sources)
    