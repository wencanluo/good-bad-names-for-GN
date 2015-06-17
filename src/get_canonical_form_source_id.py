from gni import GNI_DB
import fio
import os

def get_canonical_forms_with_id(db, output):
    import sys
    sys.stdout = open(output, 'w')
    
    for row in db.get_canonical_forms_with_id(limit = None):
        print row[0], '\t', row[1]
    
def get_name_string_indices(db, output):
    import sys
    sys.stdout = open(output, 'w')
    
    for row in db.get_name_string_indices(limit = None):
        process_name(row)
    
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
    
def select_canonical_form_source_id_row_by_row(db):
    head = ['canonical_name', 'source_id', 'has_classification_path', 'match_classification_path']
    file = datadir + 'canonical_form_source_id.txt'
    import sys
    sys.stdout = open(file, 'w')
    
    fio.PrintList(head)
    
    start_n = 0
    n = 100000
    
    sql = 'select canonical_forms.name, name_string_indices.data_source_id, name_string_indices.classification_path from name_strings join canonical_forms on name_strings.canonical_form_id = canonical_forms.id join name_string_indices on name_strings.id = name_string_indices.name_string_id '
                    
    while True:
        cur_sql = sql + 'Limit ' + str(start_n) + ',' + str(n)
        
        try:
            # Execute the SQL command
            cursor = db.execute_sql(cur_sql)
            N = cursor.rowcount
            #print 'N=', N
            
            if N == 0: break
            
            for i in range(N):
                names = cursor.fetchone()
                
                process_name(names)
            
        except:
            print "Error: unable to fecth data"
            
        start_n = start_n + n
        

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
    
    #names = db.get_canonical_forms_with_source_id(limit=None)
    
    #select_canonical_form_source_id_row_by_row(db)
    
    #output = os.path.join(datadir, 'canonical_forms_with_id.txt')
    #get_canonical_forms_with_id(db, output)
    
    output = os.path.join(datadir, 'name_string_indices.txt')
    get_name_string_indices(db, output)
    
    