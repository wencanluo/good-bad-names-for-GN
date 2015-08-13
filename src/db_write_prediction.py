import os
import itertools as it
import codecs
import file_util as fio
from warnings import filterwarnings
import MySQLdb
filterwarnings('ignore', category = MySQLdb.Warning)
from gni import GNI_DB

from db_merge_all_features import generator_ids

def add_badname_with_stringissue(db, missed_id):
    count = 0
    
    keys = ['good_bad', 'comment']
    
    for id in generator_ids(missed_id):
        features = ['0', "'enc'"]
        
        assert(len(features) == len(keys))
        key_feature_pair = []
        for key, feature in it.izip(keys, features):
            key_feature_pair.append('='.join([key, feature]))
        
        sql = 'UPDATE name_string_refinery SET %s WHERE id = %d' % (','.join(key_feature_pair), id)
        if debug:
            print sql
        
        try:
            cursor = db.execute_sql(sql)
            N = cursor.rowcount
        except Exception as e:
            print e
            print dict
            print "Error: unable to insert data"
        
        count += 1
        
        if count%10000 == 0:
            print count
            db.commit()
        
        if debug:
            if count > 1: break 
        
    db.commit()
    
    print "total count:", count

def add_simplebadnames(db):
    count = 0
    
    keys = ['good_bad', 'comment']
    
    for row in db.get_simple_badname_ids(limit=None):
        id, comment = row
        
        if comment == None:
            new_comment = "'rules'"
        else:
            comments = comment.split(", ")
            if 'rules' in comments: continue
            
            new_comment = "'%s, rules'" % comment
            
        features = ['0', new_comment]
        
        assert(len(features) == len(keys))
        key_feature_pair = []
        for key, feature in it.izip(keys, features):
            key_feature_pair.append('='.join([key, feature]))
        
        sql = 'UPDATE name_string_refinery SET %s WHERE id = %d' % (','.join(key_feature_pair), id)
        if debug:
            print sql
        
        try:
            cursor = db.execute_sql(sql)
            N = cursor.rowcount
        except Exception as e:
            print e
            print dict
            print "Error: unable to insert data"
        
        count += 1
        
        if count%10000 == 0:
            print count
            db.commit()
        
        if debug:
            if count > 1: break 
        
    db.commit()
    
    print "total count:", count

def get_weka_files(db):
    batch_size = 100
    
    
    
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
    
    missed_id = os.path.join(datadir, 'missed_id.txt')
    #add_badname_with_stringissue(db, missed_id)
    
    add_simplebadnames(db)
    
    #simple_badname_id = os.path.join(datadir, 'simple_badname_id.txt')
    #add_simplebadnames_ids(db, simple_badname_id)
    