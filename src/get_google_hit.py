#get good hit for all the names
import sys, os
from google import GoogleSearchEngine
import fio

from parallel_wrapper import *

gse = GoogleSearchEngine()

def process_batch(fout, ids, names):
    hits = task_parallel(gse.get_google_hit, names)
    
    for id, hit in zip(ids, hits):
        fout.write(str(id))
        fout.write('\t')
        fout.write(str(hit))
        fout.write('\n')
    fout.flush()
    
def get_google_hit(canonical_forms_with_id, output):    
    batch_ids = []
    batch_names = []
    
    dict = fio.LoadDict(output, 'str')
    
    fout = open(output, 'a')
    
    line_count = 0
    for line in open(canonical_forms_with_id):
        line = line.strip()
        tokens = line.split('\t')
        canonical_forms_id = tokens[0]
        canonical_forms_name = tokens[1]
        
        if canonical_forms_id in dict and dict[canonical_forms_id] != 0: continue
        
        line_count += 1
        print line_count
        
        batch_ids.append(canonical_forms_id)
        batch_names.append(canonical_forms_name)
        
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
    
    #db = GNI_DB(host=host, user=user, passwd=passwd, db=db)
    
    canonical_forms_with_id = os.path.join(datadir, 'canonical_forms_with_id.txt')
    canonical_forms_with_google_hit = os.path.join(datadir, 'canonical_forms_with_google_hit.txt')
    get_google_hit(canonical_forms_with_id, canonical_forms_with_google_hit)