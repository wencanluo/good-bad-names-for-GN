#get good hit for all the names
import sys, os
import fio
import codecs
from pyxdameraulevenshtein import damerau_levenshtein_distance as dl_distance

def get_names(input):
    names = []
    for line in codecs.open(input, 'r', 'utf-8'):
        name, freq = line.split('\t')
        name = name.strip()
        
        names.append(name)
    return names
    
def get_suggest_name(input):
    fout = input + '.suggested_by_dl'
    
        
        
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
    species = os.path.join(datadir, 'species.txt')
    
    get_suggest_name(genus)