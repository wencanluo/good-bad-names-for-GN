from gni import GNI_DB
import json
import fio
import codecs
from collections import defaultdict
from gn_parser import Parser

def extract_parser_features_from_string(data_string):
    data = json.loads(data_string, encoding = 'utf-8')
    
    parsed_data = data['scientificName']
    
    parsed = parsed_data['parsed']
    
    name_string = parsed_data['verbatim'] if 'verbatim' in parsed_data else ''
    
    has_canonical = 'canonical' in parsed_data
    has_species = 'details' in parsed_data and len(parsed_data['details']) > 0 and 'species' in parsed_data['details'][0]
    has_basionymAuthorTeam = has_species and 'basionymAuthorTeam' in parsed_data['details'][0]['species']
    has_author = has_basionymAuthorTeam and 'author' in parsed_data['details'][0]['species']['basionymAuthorTeam']
    has_year = has_basionymAuthorTeam and 'year' in parsed_data['details'][0]['species']['basionymAuthorTeam']
    hybrid =  parsed_data['hybrid']==True if 'hybrid' in parsed_data else 'unk'
    parse_run = parsed_data['parser_run'] if 'parser_run' in parsed_data else 'unk'
    surrogate = parsed_data['surrogate'] == True if 'surrogate' in parsed_data else 'unk' 
    has_ignored = 'details' in parsed_data and len(parsed_data['details']) > 0 and 'ignored' in parsed_data['details'][0]
    
    return name_string, parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored 
    
def get_parser_data(db, parser_output):
    #sys.s = codecs.open(parser_output, 'w', 'utf-8')
    import sys
    sys.stdout = codecs.open(parser_output, 'w', 'utf-8')
    
    for row in db.get_parsed_name_strings(limit = None):
        id, data = row
        name_string, parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored = extract_parser_features_from_string(data)
        
        name_string = name_string.replace('\t', ' ')
        name_string = name_string.replace('\n', ' ')
        name_string = name_string.replace('\r', ' ')
        
        row = [id, name_string, parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored]
        
        fio.PrintListSimple(row)
    
def get_simple_bad_names(parser_output, datadir):
    import sys
    sys.stdout = codecs.open(os.path.join(datadir, 'not_processed.txt'), 'w', 'utf-8')
    
    rules = ['question_mark', 'not_parsed', 'hybrid', 'surrogate', 'has_ignored']
    fout = []
    for rule in rules:
        fout.append(codecs.open(os.path.join(datadir, 'bad_'+rule+'.txt'), 'w', 'utf-8'))
    
    dict = defaultdict(int)
    
    line_count = 0
    for line in codecs.open(parser_output, 'r', 'utf-8'):
        row = line.split('\t')
        
        try:
            [id, name_string, parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored] = row
        except ValueError:
            print line
            continue
        
        line_count += 1
        
        #if line_count > 100000: break
        
        #print row
        rule_indictors = ['?' in name_string.rstrip(), parsed.rstrip() != 'True', hybrid.rstrip() == 'True', surrogate.rstrip() == 'True', has_ignored.rstrip() == 'True']
        
        dict['all'] += 1
        
        bad = False
        for i, indictor in enumerate(rule_indictors):
            if indictor:
                fout[i].write(id)
                fout[i].write('\t')
                fout[i].write(name_string)
                fout[i].write('\n')
                
                dict[rules[i]] += 1
                bad = True
            
        if bad:
            dict['bad'] += 1
    
    for i in range(len(rules)):
        fout[i].close()
        
    fio.SaveDict(dict, os.path.join(datadir, 'bad_name_count.txt'), True)

def gather_data_info(parser_output, datadir):
    #sys.s = codecs.open(parser_output, 'w', 'utf-8')
    infos = ['genus', 'species', 'author']
    author_index = infos.index('author')
    
    dicts = []
    for info in infos:
        dicts.append(defaultdict(int))
        
    parser = Parser()
    for row in db.get_parsed_name_strings(limit = None):
        id, data = row
        
        for info in parser.extract_info(data):
            if info == None: continue
            
            genus, species, authors = info
            
            for i, item in enumerate([genus, species]):
                if item == None: continue
                dicts[i][item] += 1 
            
            if authors != None:
                for item in authors:
                    if item == None: continue
                    dicts[author_index][item] += 1
                
    for i, info in enumerate(infos):
        fio.SaveDictSimple(dicts[i], os.path.join(datadir, 'info_'+info+'.txt'), SortbyValueflag=True)
           
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
    
    import os
    parser_output = os.path.join(datadir, 'parser_info.txt')
    
    #get_parser_data(db, parser_output)
    #get_simple_bad_names(parser_output, datadir)

    gather_data_info(parser_output, datadir)
    