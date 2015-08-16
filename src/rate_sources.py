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

def get_bad_name_info(db, output):
    import sys
    oldstdout = sys.stdout
    sys.stdout = codecs.open(output, 'w', 'utf-8')
        
    count = 0
    for row in db.get_bad_names(limit = None):
        fio.PrintListSimple(row)
        
        count += 1
        
        if debug:
            if count > 10: break
            
    sys.stdout.close()
    sys.stdout = oldstdout

def get_name_source_info(db, output):
    import sys
    oldstdout = sys.stdout
    sys.stdout = codecs.open(output, 'w', 'utf-8')
        
    count = 0
    for row in db.get_name_source(limit = None):
        fio.PrintListSimple(row)
        
        count += 1
        
        if debug:
            if count > 10: break
            
    sys.stdout.close()
    sys.stdout = oldstdout
 
def get_sample_badname(db, bad_name_info):
    
    count = 0
    
    examples = []
    
    for line in codecs.open(bad_name_info, 'r', 'utf-8'):
        line = line.rstrip('\r\n')
        
        name_id, synonym, data_sources, comment = line.split('\t')
        name_id = int(name_id)
        
        if synonym == '0' and comment == 'predict':
            name_string = db.get_name_string_from_id(name_id)
            examples.append(name_string)
        
        count += 1
        
        if count > 10000: break
    
    shuffle(examples)
    
    selected_examples = sorted(examples[0:100])
    
    print '\n'.join(selected_examples)
 
def rate_sources(db, name_source_info, output):
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
    
    bad_name_info = os.path.join(datadir, 'bad_name_info.txt')
    #get_bad_name_info(db, bad_name_info)
    #get_sample_badname(db, bad_name_info)
    
    name_source_info = os.path.join(datadir, 'name_source_info.txt')
    #get_name_source_info(db, name_source_info)
    
    name_source_rate = os.path.join(datadir, 'name_source_rate.txt')
    #rate_sources(db, name_source_info, name_source_rate)
    