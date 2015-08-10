import sys, os
import file_util as fio
import codecs
from collections import defaultdict

def get_names(input):
    names = []
    for line in codecs.open(input, 'r', 'utf-8'):
        name, freq = line.split('\t')
        name = name.strip()
        
        names.append(name)
    return names

def get_rank_dict(input):
    dict = {}
    for i, line in enumerate(codecs.open(input, 'r', 'utf-8')):
        tokens = line.rstrip('\r\n').split('\t')
        name = tokens[0].strip()
        
        dict[name] = i+1
        
    return dict

def get_dict(input):
    dict = {}
    for line in codecs.open(input, 'r', 'utf-8'):
        tokens = line.rstrip('\r\n').split('\t')
        name = tokens[0].strip()
        
        if len(tokens) > 1 and len(tokens[1]) > 0:
            dict[name] = tokens[1].strip()
        else:
            dict[name] = None
        
    return dict
    
def check_google(names, dict_name_lower):
    '''
    check whether the google suggestion is among the predefined name list
    '''
    for name in names.split():
        if name in dict_name_lower:
            return True
    
    return False

def check_combine(names_google, names_ld):
    '''
    check whether the google and dl give the same suggestion
    '''
    names_ld = [name.lower() for name in names_ld.split()]
    for name in names_google.split():
        if name in names_ld:
            return True
    
    return False
    
def get_miss_spelled(names, names_google, names_ld, output):
    dict_name = get_dict(names)
    
    for k, v in dict_name.items():
        dict_name[k] = int(v)
    
    dict_name_lower = {}
    for k, v in dict_name.items():
        dict_name_lower[k.lower()] = int(v)
        
    if names_google != None:
        dict_google = get_dict(names_google)
    
    if names_ld != None:
        dict_ld = get_dict(names_ld)
    
    dict_count = defaultdict(int)
    head = ['name', 'count', 'by google', 'by dl']
    
    body = []
    for name in sorted(dict_name, key=dict_name.get, reverse = True):
        row = [name, dict_name[name]]
        
        dict_count['total'] += 1
        dict_count['total_freq'] +=dict_name[name]
        
        flag_google = False
        flag_ld = False
        
        if names_google:
            if dict_google[name] != None: 
                dict_count['google'] += 1
                flag_google = True
        if names_ld:
            if dict_ld[name] != None: 
                dict_count['ld'] += 1
                flag_ld = True
        
        if flag_google and flag_ld:
            dict_count['both'] += 1
            
        if not flag_google and not flag_ld:
            dict_count['good name'] += 1
            dict_count['good name freq'] += dict_name[name]
            
            continue
        
        if flag_google:
            row.append(dict_google[name])
        else:
            row.append("")
            
        if flag_ld:
            row.append(dict_ld[name])
        else:
            row.append("")
        
        #if flag_google and not check_google(dict_google[name], dict_name_lower): continue
        
        if flag_google and flag_ld:
            if not check_combine(dict_google[name], dict_ld[name]): continue
            body.append(row)
        
    fio.WriteMatrix(output, body, head)
    
    fio.SaveDict(dict_count, names+'.count')
    
if __name__ == '__main__':
    import ConfigParser
    
    config = ConfigParser.RawConfigParser()
    config.read('../config/myconfig.cfg')
    
    host = config.get('mysql', 'host')
    user = config.get('mysql', 'user')
    passwd = config.get('mysql', 'passwd')
    db = config.get('mysql', 'db')
    
    datadir = config.get('dir', 'data')
    genus = os.path.join(datadir, 'genus.txt')
    genus_google = os.path.join(datadir, 'genus_spell_checked.txt')
    genus_ld = os.path.join(datadir, 'genus_suggested_by_dl.txt')
    output = os.path.join(datadir, 'genus_suggested.txt')
    
    get_miss_spelled(genus, genus_google, genus_ld, output)    