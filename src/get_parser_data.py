from gni import GNI_DB
import json
import fio
import codecs
from collections import defaultdict
from gn_parser import Parser
 
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

def gather_data_info(db, datadir):
    #sys.s = codecs.open(parser_output, 'w', 'utf-8')
    infos = ['genus', 'species', 'author']
    author_index = infos.index('author')
    
    dicts = []
    for info in infos:
        dicts.append(defaultdict(int))
    
    keys = ['genus', 'species', 'authors']
    parser = Parser()
    for row in db.get_parsed_name_strings(limit = None):
        id, data = row
        
        for info in parser.extract_info(data):
            if info == None: continue
            
            genus, species, authors = [info[key] for key in keys]
            
            for i, item in enumerate([genus, species]):
                if item == None: continue
                dicts[i][item] += 1 
            
            if authors != None:
                for item in authors:
                    if item == None: continue
                    dicts[author_index][item] += 1
                
    for i, info in enumerate(infos):
        fio.SaveDictSimple(dicts[i], os.path.join(datadir, 'info_'+info+'.txt'), SortbyValueflag=True)

def gather_data_info_genus_species(db, datadir):
    #sys.s = codecs.open(parser_output, 'w', 'utf-8')
    infos = ['genus', 'species']
    
    dict = defaultdict(int)
    
    keys = ['genus', 'species']
    parser = Parser()
    for row in db.get_parsed_name_strings(limit = 100):
        id, data = row
        
        for info in parser.extract_info(data):
            if info == None: continue
            
            genus, species = [info[key] for key in keys]
            
            if genus == None: continue
            if species == None: continue
            
            dict[format_genus_species(genus, species)] += 1
                
    fio.SaveDictSimple(dict, os.path.join(datadir, 'info_genus_species.txt'), SortbyValueflag=True)

def format_genus_species(genus, species):
    return genus + ' ' + species
            
def format_authors_year2(authors, year):
    if authors != None:
        author_lower = [author.lower() for author in authors]
    else:
        author_lower = ['unk']
    
    if year == None:
        year = '0000'
    
    return '&'.join(author_lower) + ', ' + year

def format_authors_year(authors, year):
    dict = {}
    dict['authors'] = authors
    dict['year'] = year
    
    return json.dumps(dict)
    
def get_same_name_but_different_author(db, datadir):
    parser = Parser()
    keys = ['authors', 'year', 'canonical']
    
    dict = {}
    
    for row in db.get_parsed_name_strings(limit = None):
        id, data = row
        
        for info in parser.extract_info(data):
            if info == None: continue
            
            authors, year, canonical = [info[key] for key in keys]
            
            if canonical == None: continue
            if authors == None: continue
            if year == None: continue
            
            if canonical not in dict:
                dict[canonical] = defaultdict(int)
            
            dict[canonical][format_authors_year(authors, year)] += 1
    
    diff_dict = {}
    for k, v in dict.items():
        if len(v) > 1:
            diff_dict[k] = v
        
    with codecs.open(os.path.join(datadir, 'same_name_but_different_author.txt'), 'w', 'utf-8') as fout:
        json.dump(diff_dict, fout, indent=2)

def get_same_name_but_different_author_for_VertNet(db, datadir, vernet_canonical):
    parser = Parser()
    keys = ['authors', 'year', 'canonical']
    
    with codecs.open(vernet_canonical, 'r', 'utf-8') as fin:
        vernet_names = json.load(fin)
    
    cannoical_dict = {}
    for k, v in vernet_names.items():
        cannoical_dict[v] = k
    
    dict = {}
    
    for row in db.get_parsed_name_strings(limit = None):
        id, data = row
        
        for info in parser.extract_info(data):
            if info == None: continue
            
            authors, year, canonical = [info[key] for key in keys]
            
            if canonical == None: continue
            if authors == None: continue
            if year == None: continue
            
            if canonical not in cannoical_dict: continue
            print canonical
            
            if canonical not in dict:
                dict[canonical] = defaultdict(int)
            
            dict[canonical][format_authors_year(authors, year)] += 1
    
    with codecs.open(os.path.join(datadir, 'same_name_but_different_author_vertnet.json'), 'w', 'utf-8') as fout:
        json.dump(dict, fout, indent=2)
        
def get_different_name_but_same_author(db, datadir):
    parser = Parser()
    keys = ['authors', 'year', 'canonical']
    
    dict = {}
    
    for row in db.get_parsed_name_strings(limit = None):
        id, data = row
        
        for info in parser.extract_info(data):
            if info == None: continue
            
            authors, year, canonical = [info[key] for key in keys]
            
            if canonical == None: continue
            if authors == None: continue
            if year == None: continue
            
            author_year = format_authors_year(authors, year)
            if author_year not in dict:
                dict[author_year] = defaultdict(int)
            
            dict[author_year][canonical] += 1
    
    diff_dict = {}
    for k, v in dict.items():
        if len(v) > 1:
            diff_dict[k] = v
            
    with codecs.open(os.path.join(datadir, 'different_name_but_same_author.txt'), 'w', 'utf-8') as fout:
        json.dump(diff_dict, fout, indent=2)
                
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

    #gather_data_info_genus_species(db, datadir)
    
    #get_same_name_but_different_author(db, datadir)
    #get_different_name_but_same_author(db, datadir)
    
    vernet_canonical = os.path.join(datadir, 'vernet_canonical.json')
    
    get_same_name_but_different_author_for_VertNet(db, datadir, vernet_canonical)