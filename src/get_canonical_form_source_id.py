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
    
    head = ['canonical_name', 'source_id', 'classification_path']
    
    body = []
    for name in names:
        row = [name[0], name[1], name[2]]
        body.append(row)
    fio.WriteMatrix(datadir + 'canonical_form_source_id.txt', body, head)