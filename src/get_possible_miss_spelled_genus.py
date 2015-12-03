#get good hit for all the names
import sys, os
from google import GoogleSearchEngine
import fio
import codecs

from parallel_wrapper import *

gse = GoogleSearchEngine()

def process_batch(fout, ids, names):
    hits = task_parallel(gse.didYouMean, names)
    
    for id, hit in zip(ids, hits):
        fout.write(str(id))
        fout.write('\t')
        fout.write(str(hit))
        fout.write('\n')
    fout.flush()

def load_result(input):
    dict = {}
    for line in codecs.open(input, 'r', 'utf-8'):
        tokens = line.split('\t')
        
        name = tokens[0].strip()
        dict[name] = True
         
    return dict
        
def get_possible_miss_spelled_genus(genus_input, output):    
    batch_ids = []
    batch_names = []
    
    dict = load_result(output)
    
    sys.stdout = codecs.open(output, 'a', 'utf-8')
    
    line_count = 0
    for i, line in enumerate(codecs.open(genus_input, 'r', 'utf-8')):
        genus = line.strip()
        
        if genus in dict: continue
        
        try:
            genus = str(genus)
        except:
            continue
        
        hit = gse.didYouMean(genus)
        
        print genus, '\t', hit
        
        sys.stdout.flush()
        
def get_possible_miss_spelled_genus_batch(genus_input, output):    
    batch_ids = []
    batch_names = []
    
    fout = codecs.open(output, 'w', 'utf-8')
    
    line_count = 0
    for i, line in enumerate(codecs.open(genus_input, 'r', 'utf-8')):
        genus = line.strip()
        
        try:
            genus = str(genus)
        except:
            continue
        
        line_count += 1
        print line_count
        
        batch_ids.append(i)
        batch_names.append(genus)
        
        if line_count % 10 == 0:
            #get the results
            process_batch(fout, batch_ids, batch_names)
            
            batch_ids = []
            batch_names = []
    
    #last batch
    process_batch(fout, batch_ids, batch_names)
    fout.close()
        
if __name__ == '__main__':
    import ConfigParser
    
    config = ConfigParser.RawConfigParser()
    config.read('../config/myconfig.cfg')
    
    host = config.get('mysql', 'host')
    user = config.get('mysql', 'user')
    passwd = config.get('mysql', 'passwd')
    db = config.get('mysql', 'db')
    
    datadir = config.get('dir', 'data')
    genus_input = os.path.join(datadir, 'genus.txt')
    genus_output = os.path.join(datadir, 'genus_suggested_by_google.txt')
    
    get_possible_miss_spelled_genus(genus_input, genus_output)