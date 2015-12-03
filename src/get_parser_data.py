from gni import GNI_DB
import json
import file_util as fio
import codecs
from collections import defaultdict
from gn_parser import Parser
import sys

def get_parser_data(db, parser_output):
    #sys.s = codecs.open(parser_output, 'w', 'utf-8')
    import sys
    sys.stdout = codecs.open(parser_output, 'w', 'utf-8')
    
    parser = Parser()
    for row in db.get_parsed_name_strings(limit = None):
        id, data = row
        name_string, parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored = parser.extract_parser_features_from_string(data)
        
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

def get_all_name_strings(parser_output, output):
    '''
    Get the names of the names
    '''
    fout = codecs.open(output, 'w', 'utf-8')
    
    line_count = 0
    for line in codecs.open(parser_output, 'r', 'utf-8'):
        row = line.split('\t')
        
        try:
            [id, name_string, parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored] = row
            
            line_count += 1
            #fout.write(id)
            #fout.write('\t')
            fout.write(name_string)
            fout.write('\r\n')
            
            #if line_count > 100: break
            
        except ValueError:
            print line
            continue
    
    print "total name", line_count
    
    fout.close()

def get_all_ids(parser_output, output):
    '''
    Get only the id of the names
    '''
    
    fout = codecs.open(output, 'w', 'utf-8')
    
    line_count = 0
    for line in codecs.open(parser_output, 'r', 'utf-8'):
        row = line.split('\t')
        
        try:
            [id, name_string, parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored] = row
            
            line_count += 1
            fout.write(id)
            fout.write('\r\n')
            
        except ValueError:
            print line
            continue
    
    print "total name", line_count
    
    fout.close()
    
def check_id_order(all_name_string_id):
    old_id = 0
    for line in open(all_name_string_id):
        line = line.rstrip('\r\n')
        
        tokens = line.split('\t')
        name_string_id, data_source_id, has_classification_path, is_synonym = tokens
        name_string_id = int(name_string_id)
        
        if name_string_id < old_id:
            print '@'
            break
        
        old_id = name_string_id
    
    print "good"
        
    
def combine_name_sources(ids_file, all_name_string_id, all_name_strings_id_combined):
    old_id = 0
    
    sys.stdout = codecs.open(all_name_strings_id_combined, 'w', 'utf-8')
    
    dict = None
    #process name_string_indices
    line_count = 0
    for line in open(all_name_string_id):
        line = line.rstrip('\r\n')
        
        tokens = line.split('\t')
        name_string_id, data_source_id, has_classification_path, is_synonym = tokens
        name_string_id = int(name_string_id)
        
        if name_string_id != old_id:
            #save dict
            if dict != None:
                row = [old_id, ','.join(dict['s']), dict['c'], dict['n']] #name_string_id
                fio.PrintList(row)
    
            dict = {'s':[], 'c':False, 'n':False}
            
        dict['s'].append(data_source_id)
        dict['c'] = dict['c'] or (has_classification_path == 'True')
        dict['n'] = dict['n'] or (is_synonym == 'True')
        
        old_id = name_string_id
        
        line_count += 1
        
        #if line_count > 10: break
        
    row = [old_id, ','.join(dict['s']), dict['c'], dict['n']] #name_string_id
    fio.PrintList(row)
    
    sys.stdout.close()

def get_all_name_strings_id(parser_output, output):
    fout = codecs.open(output, 'w', 'utf-8')
    
    line_count = 0
    for line in codecs.open(parser_output, 'r', 'utf-8'):
        row = line.split('\t')
        
        try:
            [id, name_string, parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored] = row
            
            line_count += 1
            #fout.write(id)
            #fout.write('\t')
            fout.write(id)
            fout.write('\r\n')
            
            #if line_count > 100: break
            
        except ValueError:
            print line
            continue
    
    print "total name", line_count
    
    fout.close()
        
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
        fio.SaveDictSimple(dicts[i], os.path.join(datadir, info+'.txt'), SortbyValueflag=True)

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
    
    '''
    diff_dict = {}
    for k, v in dict.items():
        if len(v) > 1:
            diff_dict[k] = v
    '''
    
    with codecs.open(os.path.join(datadir, 'same_name_but_different_author_all.txt'), 'w', 'utf-8') as fout:
        json.dump(dict, fout, indent=2)

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
    
    #Exact the name list
    parser_output = os.path.join(datadir, 'parser_info.txt')
    get_parser_data(db, parser_output)
    
    all_name_string = os.path.join(datadir, 'all_name_strings.txt')
    get_all_name_strings(parser_output, all_name_string)
    
    id_file = os.path.join(datadir, 'all_name_strings.id_list')
    get_all_ids(parser_output, id_file)
    
    #get_simple_bad_names(parser_output, datadir)

    gather_data_info_genus_species(db, datadir)
    
    #get_same_name_but_different_author(db, datadir)
    #get_different_name_but_same_author(db, datadir)
    
    #vernet_canonical = os.path.join(datadir, 'vernet_canonical.json')
    
    #get_same_name_but_different_author_for_VertNet(db, datadir, vernet_canonical)
    
    all_name_string_id = os.path.join(datadir, 'name_string_all_indices.txt')
    get_all_name_strings_id(parser_output, all_name_string_id)
    
    all_name_strings_id_combined = os.path.join(datadir, 'all_name_strings_id_combined.txt')
    combine_name_sources(id_file, all_name_string_id, all_name_strings_id_combined)
    