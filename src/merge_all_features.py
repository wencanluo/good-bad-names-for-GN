import os
import itertools as it
import codecs
import file_util as fio
from warnings import filterwarnings
import MySQLdb
filterwarnings('ignore', category = MySQLdb.Warning)
from gni import GNI_DB

def generator_ids(input):
    for line in codecs.open(input, 'r', 'utf-8'):
        line = line.rstrip('\r\n')
        tokens = line.split('\t')
        yield int(tokens[0])
    
def generator_features(input):
    for line in codecs.open(input, 'r', 'utf-8'):
        line = line.rstrip('\r\n')
        tokens = line.split('\t')
        yield tokens

def add_netineti(db, id_list, netineti):
    count = 0
    for id, neti in it.izip(generator_ids(id_list), generator_features(netineti)):
        
        id = int(id)
        
        if neti == 'Yes':
            neti = 1
        elif neti == 'No':
            neti = -1
        else:
            neti = 0
                
        sql = 'UPDATE name_string_refinery SET netineti = %s WHERE id = %d' % (neti, id)
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

def add_taxon(db, id_list, taxon_file):
    count = 0
    for id, taxon in it.izip(generator_ids(id_list), generator_features(taxon_file)):
        
        id = int(id)
        
        if taxon == 'Yes':
            taxon = 1
        elif taxon == 'No':
            taxon = -1
        else:
            taxon = 0
                
        sql = 'UPDATE name_string_refinery SET taxonfinder = %s WHERE id = %d' % (taxon, id)
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
    
def add_features(db, id_list, feature_file, keys):
    count = 0
    for id, features in it.izip(generator_ids(id_list), generator_features(feature_file)):
        id = int(id)
        
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

if __name__ == '__main__':
    import ConfigParser
    
    config = ConfigParser.RawConfigParser()
    config.read('../config/myconfig.cfg')
    
    host = config.get('mysql', 'host')
    user = config.get('mysql', 'user')
    passwd = config.get('mysql', 'passwd')
    db = config.get('mysql', 'db')
    
    datadir = config.get('dir', 'data')
    
    netineti = os.path.join(datadir, 'all_name_strings.NetiNeti')
    taxon = os.path.join(datadir, 'all_name_strings.taxon')
    
    simple_features = os.path.join(datadir, 'all_name_strings.simple')
    misspelling_authorship = os.path.join(datadir, 'all_name_strings.misspelling_authorship')
    
    id_list = os.path.join(datadir, 'all_name_strings.id_list')
    #get_id_list(misspelling_authorship, id_list)
    
    db = GNI_DB(host=host, user=user, passwd=passwd, db=db)
    
    debug = True
    #add_netineti(db, id_list, netineti)
    add_taxon(db, id_list, taxon)
    add_features(db, id_list, simple_features, ['has_question_mark', 'all_caplitized'])
    