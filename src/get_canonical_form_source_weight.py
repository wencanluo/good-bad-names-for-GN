from gni import GNI_DB
import json
from source_authority import SourceAuthority 

def get_weight_and_insert_to_db(db, combine_canonical_form_with_sources, source_authority):
    authority = SourceAuthority(source_authority)
    
    for i, line in enumerate(open(combine_canonical_form_with_sources)):
        tokens = line.rstrip('\r\n').split('\t')
        
        dict = {}
        
        dict['id'] = tokens[0]
        
        sources = tokens[1].split(',')
        
        dict['num_sources'] = len(sources)
        dict['source_nomenclatural_authority_total'] = authority.get_nomenclatural_authority_total(sources)
        dict['source_nomenclatural_authority_max'] = authority.get_nomenclatural_authority_max(sources)
        dict['source_taxonomic_authority_total'] = authority.get_taxonomic_authority_total(sources)
        dict['source_taxonomic_authority_max'] = authority.get_taxonomic_authority_max(sources)
        
        dict['has_path'] = 1 if tokens[2] == 'True' else 0
        dict['match_path'] = 1 if tokens[3] == 'True' else 0
        
        for k, v in dict.items(): #for sql
            dict[k] = str(v)
        sql = 'INSERT INTO canonical_forms_refinery (' + ','.join(dict.keys()) + ') VALUES (' +','.join(dict.values())+ ')'
        
        try:
            cursor = db.execute_sql(sql)
            N = cursor.rowcount
        except:
            print "Error: unable to insert data"
        
        if i%10000 == 0:
            print i
            db.commit()
    db.commit()
    
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
    combine_canonical_form_with_sources = os.path.join(datadir, 'canonical_forms_id_with_sources.txt')
    source_authority = os.path.join(datadir, 'source_authority.txt')
    
    get_weight_and_insert_to_db(db, combine_canonical_form_with_sources, source_authority)
    
    
    