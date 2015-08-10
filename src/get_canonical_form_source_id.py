from gni import GNI_DB
import fio
import os
import sys

def get_canonical_forms_with_id(db, output):
    import sys
    sys.stdout = open(output, 'w')
    
    for row in db.get_canonical_forms_with_id(limit = None):
        print row[0], '\t', row[1]
    
def get_name_string_indices(db, output):
    import sys
    sys.stdout = open(output, 'w')
    
    for data in db.get_name_string_indices(limit = None):
        name_string_id, data_source_id, classification_path, taxon_id, accepted_taxon_id, synonym = data
        
        has_classification_path = False
        
        if classification_path != None:
            classification_path = classification_path.strip()
        else:
            classification_path = ""
        
        if len(classification_path) > 0:
            has_classification_path = True
        
        is_synonym = False
        if taxon_id != accepted_taxon_id:  is_synonym = True
        
        row = [name_string_id, data_source_id, has_classification_path, is_synonym]
        
        fio.PrintList(row)

def get_name_string(db, output):
    import sys
    sys.stdout = open(output, 'w')
    
    for row in db.get_name_string(limit = None):
        print row[0], '\t', row[1]
            
def process_name(name):
    row = [name[0], name[1]]
    
    if name[2] != None:
        classification_path = name[2].strip()
    else:
        classification_path = ""
    
    if len(classification_path) > 0:
        row.append(True)
    else:
        row.append(False)
    
    k = classification_path.rfind('|')
    if k != -1:
        last_classification_path  = classification_path[k+1:]
    else:
        last_classification_path = classification_path
    
    row.append(last_classification_path)
#     if name[0].lower() == last_classification_path.lower():
#         row.append(True)
#     else:
#         row.append(False)
    
    fio.PrintList(row)
    
def get_canonical_form_source_id(db, output):
    head = ['canonical_name', 'source_id', 'has_classification_path', 'match_classification_path']
    sys.stdout = open(output, 'w')
    
    fio.PrintList(head)
    
    for row in db.get_canonical_forms_with_source_id(limit = 100):
        process_name(row)

def combine_canonical_form_source_id(canonical_forms_with_id, name_string_indices, name_string, output):
    #read canonical_forms_with_id
    canonical_forms_with_id_dict = {}
    for line in open(canonical_forms_with_id):
        line = line.strip()
        tokens = line.split('\t')
        canonical_forms_id = int(tokens[0])
        canonical_forms_name = tokens[1]
        canonical_forms_with_id_dict[canonical_forms_id] = canonical_forms_name
    
    print "canonical_forms_with_id_dict", len(canonical_forms_with_id_dict)
    
    #read name_string
    name_string_dict = {}
    for line in open(name_string):
        line = line.strip()
        #print line
        
        tokens = line.split('\t')
        name_string_id = int(tokens[0])
        if tokens[1] == 'None': continue
        
        canonical_form_id = int(tokens[1])
        
        name_string_dict[name_string_id] = canonical_form_id
    
    print "name_string_dict", len(name_string_dict)
        
    sys.stdout = open(output, 'w')
    
    #process name_string_indices
    for line in open(name_string_indices):
        line = line.strip()
        
        tokens = line.split('\t')
        name_string_id = int(tokens[0])
        data_source_id = int(tokens[1])
        has_classification_path = (tokens[2] == 'True')
        
        if name_string_id not in name_string_dict: continue
        canonical_forms_id = name_string_dict[name_string_id]
        
        if canonical_forms_id not in canonical_forms_with_id_dict: continue
        canonical_forms_name = canonical_forms_with_id_dict[canonical_forms_id]
        
        row = [canonical_forms_id, data_source_id, has_classification_path]
        
        if has_classification_path == 1:
            if len(tokens) < 4: 
                row.append(False)
            else:
                if canonical_forms_name.lower() == tokens[3].lower():
                    row.append(True)
                else:
                    row.append(False)
        else:
            row.append(False)
        
        fio.PrintList(row)

def combine_canonical_form_sources(canonical_forms_with_source_id, combine_canonical_form_with_sources):
    dict = {}
    
    #process name_string_indices
    line_count = 0
    for line in open(canonical_forms_with_source_id):
        line = line.strip()
        
        tokens = line.split('\t')
        canonical_forms_id = int(tokens[0])
        if canonical_forms_id not in dict:
            dict[canonical_forms_id] = {}
            dict[canonical_forms_id]['sources'] = []
            dict[canonical_forms_id]['has_path'] = False
            dict[canonical_forms_id]['match_path'] = False
        
        data_source_id = tokens[1]
        dict[canonical_forms_id]['sources'].append(data_source_id)
        
        has_classification_path = (tokens[2] == 'True')
        dict[canonical_forms_id]['has_path'] = dict[canonical_forms_id]['has_path'] or has_classification_path
        
        match_classification_path = (tokens[3] == 'True')
        dict[canonical_forms_id]['match_path'] = dict[canonical_forms_id]['match_path'] or match_classification_path
        
        line_count += 1
        
        #if line_count > 10: break
    print 'total line:', line_count
    
    sys.stdout = open(combine_canonical_form_with_sources, 'w')
    
    for k, v in dict.items():
        row = [k] #canonical_forms_id
        row.append(','.join(v['sources']))
        row.append(v['has_path'])
        row.append(v['match_path'])
        
        fio.PrintList(row)
    
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
    
    canonical_forms_with_id = os.path.join(datadir, 'canonical_forms_with_id.txt')
    #get_canonical_forms_with_id(db, canonical_forms_with_id)
    
    name_string_indices = os.path.join(datadir, 'name_string_all_indices.txt')
    get_name_string_indices(db, name_string_indices)
    
    name_string = os.path.join(datadir, 'name_string.txt')
    #get_name_string(db, name_string)
    
    canonical_forms_with_source_id = os.path.join(datadir, 'canonical_forms_id_with_source_id.txt')
    #combine_canonical_form_source_id(canonical_forms_with_id, name_string_indices, name_string, canonical_forms_with_source_id)
    
    combine_canonical_form_with_sources = os.path.join(datadir, 'canonical_forms_id_with_sources.txt')
    #combine_canonical_form_sources(canonical_forms_with_source_id, combine_canonical_form_with_sources)
    