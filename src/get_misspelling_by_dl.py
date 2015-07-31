#get good hit for all the names
import sys, os
import fio
import codecs
from pyxdameraulevenshtein import damerau_levenshtein_distance as dl_distance

dl_threshold = 2

def get_names(input):
    names = []
    for line in codecs.open(input, 'r', 'utf-8'):
        name, freq = line.split('\t')
        name = name.strip()
        
        names.append(name)
    return names

def get_suggest_name_one(name, name_list):
    '''
    return closest names, only top 3 will be returned
    '''
    
    if len(name) <= dl_threshold+1: return []
    
    dict = {}
    for ref in name_list:
        if len(ref) <= dl_threshold+1: continue #short name is not considered
        
        d = dl_distance(name, ref)
        if d == 0: continue
        if d > dl_threshold: continue
        dict[ref] = d
    
    keys = sorted(dict, key=dict.get)
    return keys[:3]
    
def get_suggest_names(name_list, input, output):
    '''
    name_list: the list of the reference names that need to be checked, the names are ranked in order
    input: the list of the names that need to be checked, each line has a name and rank, separated by tab
    output: for each name, output the possible suggest names (dl_distance <= 3)
    '''
    names = get_names(name_list)
    
    body = []
    
    for line in codecs.open(input, 'r', 'utf-8'):
        token = line.split('\t')
        name = token[0].strip()
        rank = int(token[1].strip())
        
        suggested_names = get_suggest_name_one(name, names[:rank])
        body.append([name, ', '.join(suggested_names)])
    
    fio.WriteMatrix(output, body)    

if __name__ == '__main__':
    name_list = sys.argv[1]
    input = sys.argv[2]
    output = sys.argv[3]
    
    get_suggest_names(name_list, input, output)