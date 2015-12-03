import os
import itertools as it
import codecs
import file_util as fio
from warnings import filterwarnings
import MySQLdb
from numpy.core.test_rational import numerator
filterwarnings('ignore', category = MySQLdb.Warning)
from gni import GNI_DB
import sys

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
        
        db.update_table('name_string_refinery', id, features, keys)
        
        count += 1
        
        if count%10000 == 0:
            print count
            db.commit()
        
        if debug:
            if count > 1: break 
        
    db.commit()
    
    print "total count:", count

def print_head(head):
    for line in open(head):
        print line.rstrip('\r\n')

def fixed_weka_file(outputdir):
    import sys
    
    jobid = 0
    while True:
        feature_file = os.path.join(outputdir, str(jobid) + '.arff')
        if not fio.IsExist(feature_file): break
        
        lines = fio.ReadFile(feature_file)
        lines[12] = "@attribute parser_run { '0.0', '1.0', '2.0', '3.0' }"
        #print lines[12]
        
        sys.stdout = codecs.open(feature_file, 'w', 'utf-8')
        for line in lines:
            print line.rstrip('\r\n')
        
        jobid += 1
    
    sys.stdout.close()
    
def split_new_data(input, train, test):
    import random
    
    body = fio.ReadMatrix(input, hasHead=False)
    random.seed(0)
    random.shuffle(body)
    
    n = len(body)
    fio.WriteMatrix(train, body[:n/2], header=None)
    fio.WriteMatrix(test, body[n/2:], header=None)

def get_gn_weka_file(db, gn_train, gn_test):
    for input in [gn_train, gn_test]:
        output = input + '.arff'
        
        oldstdout = sys.stdout
        #sys.stdout = codecs.open(output, 'w', 'utf-8')
        sys.stdout = open(output, 'w')
            
        body = fio.ReadMatrix(input, hasHead=False)
        for row in body:
            id, label = row
            id = int(id)
            data = db.get_columns_from_a_table_by_id(id, '*', 'name_string_refinery')
            
            lstRow = [x for x in data]
            
            name = db.get_name_string_from_id(id)
            
            if input == gn_train:
                features = [id]
            else:
                features = []
            features += lstRow[1:-3] 
            
            for i in range(len(features)):
                if features[i] == None:
                    features[i] = 0
                
                if type(features[i]) == int or type(features[i]) == long:
                    features[i] = '%.1f' % float(features[i])
                    
            if row[-2] == None: 
                len_source = 0
            else:
                len_source = len(row[-2].split(','))
            
            features.append(len_source)
            
            #features.append('good')
            features.append(label.rstrip('\r\n').lower())
            
            fio.PrintListSimple(features, ',', endflag=True)
            
        sys.stdout.close()
        sys.stdout = oldstdout
        
def get_weka_files(db, outputdir):
    import sys
    oldstdout = sys.stdout
    
    batch_size = 100000
    
    head_file = os.path.join(outputdir, 'head.txt')
    
    id_file = os.path.join(outputdir, 'id.txt')
    fout_id = codecs.open(id_file, 'w', 'utf-8')
    
    count = 1
    jobid = 0
    
    feature_file = os.path.join(outputdir, str(jobid) + '.arff')
    sys.stdout = codecs.open(feature_file, 'w', 'utf-8')
    print_head(head_file)
    
    for row in db.get_all_features_from_name_string_refinery(limit=None):
        id = row[0]
        lstRow = [x for x in row]
        
        fout_id.write(str(id))
        fout_id.write('\r\n')
        
        features = lstRow[1:-3]
        
        for i in range(len(features)):
            if features[i] == None:
                features[i] = 0
            
            if type(features[i]) == int or type(features[i]) == long:
                features[i] = '%.1f' % float(features[i])
                
        if row[-2] == None: 
            len_source = 0
        else:
            len_source = len(row[-2].split(','))
        
        features.append(len_source)
        
        features.append('good')
        
        fio.PrintListSimple(features, ',')
        
        if count%batch_size == 0:#switch to new job file
            jobid += 1
            feature_file = os.path.join(outputdir, str(jobid) + '.arff')
            sys.stdout = codecs.open(feature_file, 'w', 'utf-8')
            print_head(head_file)
        
        count += 1
        
        #if jobid >= 10: break
        
    sys.stdout.close()
    fout_id.close()
    sys.stdout = oldstdout

def generate_predicted_label(outputdir):
    jobid = 0
    while True:
        feature_file = os.path.join(outputdir, str(jobid) + '.label')
        if not fio.IsExist(feature_file): break
        
        for i, line in enumerate(open(feature_file, 'r')):
            if i==0: continue
            
            tokens = line.rstrip('\r\n').split('\t')
            
            yield tokens[1]
        
        jobid += 1
    
def get_predictbad_name_ids(db, outputdir):
    id_file = os.path.join(outputdir, 'id.txt')
    
    #bad_ids = os.path.join(outputdir, 'bad_ids.txt')
    #sys.stdout = codecs.open(bad_ids, 'w', 'utf-8')
    
    keys = ['good_bad', 'comment']
    
    count = 0
    for id, label in it.izip(generator_ids(id_file), generate_predicted_label(outputdir)):
        if int(id) <= 46862650: continue
            
        row = db.get_goodbad_label(id)
        if row == None:
            print 'skip:', id
            continue
        
        good_bad, comment = row
        
        if label == 'good' and good_bad != 0: continue
        if label == 'bad' and good_bad == 0: continue
        
        if label == 'good' and good_bad == 0:
            if comment == None:
                new_comment = '""'
            else:
                comments = comment.split(", ")
                if 'rule' in comments: 
                    continue
                else:
                    new_comment = '""'
        else:#label as bad, but good_bad != 0
            if comment == None:
                new_comment = "'predict'"
            else:
                comments = comment.split(", ")
                if 'predict' in comments: continue
                
                new_comment = "'%s, predict'" % comment
        
        if label == 'good':
            features = ['1', new_comment]
        else:
            features = ['0', new_comment]
            
        db.update_table('name_string_refinery', id, features, keys)
            
        count += 1
        
        if count%10000 == 0:
        #if True:
            print count, id
            sys.stdout.flush()
            
            db.commit()
        
        if debug:
            print id
            if count > 1: break 
        
    db.commit()
    #sys.stdout.close()
    
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
    
    #add_simplebadnames(db)
    
    #simple_badname_id = os.path.join(datadir, 'simple_badname_id.txt')
    #add_simplebadnames_ids(db, simple_badname_id)
    
    new_gn_testset = os.path.join(datadir, 'gn_testset.txt')
    gn_train = os.path.join(datadir, 'gn_train.txt')
    gn_test = os.path.join(datadir, 'gn_test.txt')
    
    #split_new_data(new_gn_testset, gn_train, gn_test)
    get_gn_weka_file(db, gn_train, gn_test)
    
    outputdir = os.path.join(datadir, 'weka', 'names')
    #get_weka_files(db, outputdir)
        
    get_predictbad_name_ids(db, outputdir)