from gni import GNI_DB
import fio

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
    names = db.get_canonical_forms_with_source_id(limit=None)
    
    head = ['canonical_name', 'source_id', 'has_classification_path', 'match_classification_path']
    
    file = datadir + 'canonical_form_source_id.txt'
    import sys
    sys.stdout = open(file, 'w')
    
    fio.PrintList(head)
    
    for name in names:
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
        
        if name[0].lower() == last_classification_path.lower():
            row.append(True)
        else:
            row.append(False)
        
        #row.append(last_classification_path)
        
        fio.PrintList(row)