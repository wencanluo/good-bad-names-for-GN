import os
import itertools as it
import codecs
import file_util as fio
from warnings import filterwarnings
import MySQLdb
filterwarnings('ignore', category = MySQLdb.Warning)
from gni import GNI_DB
from gn_parser import Parser
from random import shuffle
from collections import defaultdict
import math
import json
import numpy as np

def check_ratio(body, ratio):
    ratio = float(ratio)
    
    count = 0.
    for row in body:
        name_id, good_bad, synonym = row
        if good_bad != '0' or synonym == '1': 
            count += 1.
    
    myratio = abs(count/len(body) - ratio)/ratio
    print count/len(body), ratio, myratio
    
    if myratio < 0.15: #5%
        return True
    
    return False
    
def random_pick_examples(data_sources, data_sources_ratios, outputdir):
    interested_sources = [line.rstrip('\r\n') for line in open(data_sources, 'r')]
    ratios = [line.rstrip('\r\n') for line in open(data_sources_ratios, 'r')]
    print ratios
    
    for i, source in enumerate(interested_sources):
        input = os.path.join(outputdir, source + '.txt')
        
        body = []
        for line in codecs.open(input, 'r', 'utf-8'):
            line = line.rstrip('\r\n')
            
            body.append(line.split('\t'))
        
        shuffle(body)
        while ( not check_ratio(body[:100], ratios[i]) ):
            shuffle(body)
        
        output = os.path.join(outputdir, 'picked', source + '.txt')
        fio.WriteMatrix(output, body[:100], header=None)
        
def get_annotated_examples(db, data_sources, outputdir):
    interested_sources = [line.rstrip('\r\n') for line in open(data_sources, 'r')]
    
    body = []
    
    count = 1
    for i, source in enumerate(interested_sources):
        input = os.path.join(outputdir, 'picked', source + '.txt')
        
        print source
        
        for line in codecs.open(input, 'r', 'utf-8'):
            line = line.rstrip('\r\n')
            
            name_id = line.split('\t')[0]
            name_id = int(name_id)
            
            features = list(db.get_columns_from_a_table_by_id(name_id, '*', 'name_string_refinery'))
            source_name = db.get_source_name_from_id(int(source))
            if len(source_name) > 20:
                source_name = source_name[:12]+source_name[-8:]
            
            name_string = db.get_name_string_from_id(name_id)
            classification_path = db.get_classification_path_from_id(name_id)
            
            row = [count, source, source_name, name_string] + features[:-4] + [classification_path]+ features[-4:]
            body.append(row)
            
            count += 1
            #print name_string
    
    #output = os.path.join(outputdir, 'fullname', source+'.txt')
    
    output = os.path.join(outputdir, 'picked', 'combined.txt')
    head = ['index', 'source', 'source_name', 'name_string', 
            'id', 'netineti', 'taxonfinder', 'has_question_mark', 'all_caplitized', 'parsed', 'has_canonical', 'has_species', 'has_author', 'has_year', 'hybrid', 'parser_run', 'surrogate', 'has_ignored', 'genus_freq', 'species_freq', 'genus_species_freq', 'PMI', 'genus_prob', 'species_prob', 'genus_google', 'genus_dl', 'genus_google_dl', 'species_dl', 'year_match', 'year_not_match', 'author_match', 'author_not_match', 'year_match_bigger', 'author_match_bigger', 'has_classification_path', 
            'classification_path',
            'synonym', 'good_bad', 'data_sources', 'comment', 
            ]
    fio.WriteMatrix(output, body, header=head)
            
def get_ids_from_data_sources(name_source_info, data_sources, outputdir):
    fouts = {}
    
    interested_sources = [line.rstrip('\r\n') for line in open(data_sources, 'r')]
    
    for source in interested_sources:
        output = os.path.join(outputdir, source + '.txt')
        fouts[source] = codecs.open(output, 'w', 'utf-8')

    for line in codecs.open(name_source_info, 'r', 'utf-8'):
        line = line.rstrip('\r\n')
        
        name_id, synonym, data_sources, good_bad, comment = line.split('\t')
        if data_sources == 'None': continue
        
        sources = data_sources.split(',')
        
        for source in sources:
            if source not in fouts: continue
            
            fouts[source].write(name_id + '\t' + good_bad + '\t' + synonym + '\r\n')
        
    
    for source in interested_sources:
        fouts[source].close()
    
def evaluate_rate_sources(db, name_source_info, output):
    count = 0
    
    dict = {}
    
    for line in codecs.open(name_source_info, 'r', 'utf-8'):
        line = line.rstrip('\r\n')
        
        name_id, synonym, data_sources, good_bad, comment = line.split('\t')
        if data_sources == 'None': continue
        
        sources = data_sources.split(',')
        
        for source in sources:
            if source not in dict:
                dict[source] = defaultdict(int)
        
            dict[source]['total'] += 1
        
            if good_bad != '0':#good
                dict[source]['good'] += 1
            else:
                dict[source]['bad'] += 1
                
                if comment != 'None':
                    comments = comment.split(',')
                    if 'rules' in comments:
                        dict[source]['bad_rules'] += 1
                    
                    if 'predict' in comments:
                        dict[source]['bad_predict'] += 1
                        
                if synonym != '1':
                    dict[source]['bad_not_synonym'] += 1
  
        count += 1
        
        #if count > 10000: break

    head = ['source', 'name', 'total', 'good', 'bad', 'bad_rules', 'bad_predict', 'bad_not_synonym']
    body = []
    
    for source in sorted(dict):
        source_name = db.get_source_name_from_id(int(source))
        
        if len(source_name) > 15:
            source_name = source_name[:8] + '...' + source_name[-7:]
            
        row = [source, source_name] + [dict[source][key] for key in head[2:]]
        body.append(row)
        
    fio.WriteMatrix(output, body, head)

def get_data_source_connections(name_source_info, data_source_connections):
    
    data = defaultdict(int)
    for line in codecs.open(name_source_info, 'r', 'utf-8'):
        line = line.rstrip('\r\n')
        
        name_id, synonym, data_sources, good_bad, comment = line.split('\t')
        if data_sources == 'None': continue
        
        sources = data_sources.split(',')
        
        sources = sorted([int(source) for source in sources])
        n = len(sources)
        
        if n==1:#unique for this source
            data[str(source)] += 1
        
        for source in sources:
            data['total@%s'%source] += 1
        
        for i in range(n):
            for j in range(i+1, n):
                data[str(sources[i]) + '@' + str(sources[j])] += 1
    
    with codecs.open(data_source_connections, 'w', 'utf-8') as fout:
        json.dump(data, fout, indent=2)

def matrix_source_connections(data_source_connections, output):
    with codecs.open(data_source_connections, 'r', 'utf-8') as fin:
        data = json.load(fin)
    
    #get all the sources
    allsources = []
    for key in data:
        sources = key.split('@')
        for source in sources:
            if source == 'total': continue
            allsources.append(int(source))
    
    allsources = sorted(set(allsources))
    n = len(allsources)
    
    head = [''] + list(allsources) + ['unique', 'closest']
    
    body = []
    for i in range(n):
        row = [allsources[i]]
        for j in range(n):
            if i < j:
                key = str(allsources[i]) + '@' + str(allsources[j])
            elif i == j:
                key = str(allsources[i])
            else:
                key = str(allsources[j]) + '@' + str(allsources[i])
            
            count = data[key] if key in data else 0
                
            row.append(count)
        
        total = data['total@%d'%allsources[i]]
        
        closest = i+1
        max = 0
        for k in range(1, n+1):
            if k==i+1: continue
            if row[k] >= max:
                max = row[k]
                closest = allsources[k-1]
        
        row.append(row[i+1]/float(total))
        row.append(closest)
        
        body.append(row)
    
    fio.WriteMatrix(output, body, head)

if __name__ == '__main__':
    import ConfigParser
    
    config = ConfigParser.RawConfigParser()
    config.read('../config/myconfig.cfg')
    
    host = config.get('mysql', 'host')
    user = config.get('mysql', 'user')
    passwd = config.get('mysql', 'passwd')
    db = config.get('mysql', 'db')
    
    datadir = config.get('dir', 'data')
    
    db = GNI_DB(host=host, user=user, passwd=passwd, db=db)
    
    debug = False
    
    data_sources = os.path.join(datadir, 'data_sources.txt')
    data_sources_ratios = os.path.join(datadir, 'data_sources_ratio.txt')
    
    name_source_info = os.path.join(datadir, 'name_source_info.txt')
    
    outputdir = os.path.join(datadir, 'examples')
    #get_ids_from_data_sources(name_source_info, data_sources, outputdir)
    
    #random_pick_examples(data_sources, data_sources_ratios, outputdir)
    #rate_sources(db, name_source_info, name_source_rate)
    
    #get_annotated_examples(db, data_sources, outputdir)
    
    data_source_connections = os.path.join(datadir, 'data_source_connections.json')
    #get_data_source_connections(name_source_info, data_source_connections)
    
    data_source_connections_matrix = os.path.join(datadir, 'data_source_connections_matrix.txt')
    matrix_source_connections(data_source_connections, data_source_connections_matrix)
    
    