import os
import fio
from collections import defaultdict
from gni import GNI_DB
from google import GoogleSearchEngine

class Corpus:
    def __init__(self):
        self.filelist = [#'0.txt',
                         #'1.txt',
                         #'2.txt',
                         #'3.txt',
                         #'4.txt',
                         #'5.txt',
                         #'all.txt',
                         'VertNet.txt'
                         ]
    
    def load(self, folder):
        self.string_names = []
        self.labels = []
        self.files = []
        
        for f in self.filelist:
            filename = os.path.join(folder, f)
            
            tm = []
            for i, line in enumerate(open(filename)):
                row = []
                line = line.strip()
                if len(line) == 0: continue
                
                if len(line.split("\t")) != 2:
                    print f
                    print i
                    
                for num in line.split("\t"):
                    row.append(num.strip())
                tm.append(row)
                
            name = [row[0] for row in tm]
            label = [row[1].lower() for row in tm]
            files = [f for row in tm]
        
            self.string_names += name
            self.labels += label
            self.files += files
    
    def save(self, output):
        head = ['string name', 'label', 'source']
        
        body = []
        for name, label, f in zip(self.string_names, self.labels, self.files):
            body.append([name, label, f])
        fio.WriteMatrix(output, body, head)
               
    def normalize_label(self, label):
        label_map = fio.LoadDict(label, 'str')
        
        for i, label in enumerate(self.labels):
            self.labels[i] = label_map[label]
    
    def get_distribution(self, input, output):
        head, body = fio.ReadMatrix(input, hasHead=True)
        
        source_index = head.index('source')
        label_index = head.index('label')
        
        labels = [row[label_index] for row in body]
        sources = [row[source_index] for row in body]
        
        dict = defaultdict(int)
        for source, label in zip(sources, labels):
            if label == 'good':
                dict[source] += 1
        
        fio.SaveDict(dict, output)
    
    def get_cannonical_from(self, input, output, db):
        head, body = fio.ReadMatrix(input, hasHead=True)
        
        string_name_index = head.index('string name')
        
        string_names = [row[string_name_index] for row in body]
        
        sql = 'select id, canonical_form_id from name_strings where name = '
        
        new_head = ['string_id', 'canonical_form_id'] + head
        new_body = []
        for i, string_name in enumerate(string_names):
            #print i, 
            one_sql = sql +  '"' + string_name + '"'
            
            cursor = db.execute_sql(one_sql)
            N = cursor.rowcount
            
            if N == 0:
                #row = ['None', 'None']
                print string_name
                continue
            elif N > 1:
                print N, string_name
                continue
            else:
                row = cursor.fetchone()
            
            new_row = [row[0], row[1]] + body[i]
            new_body.append(new_row)
        
        fio.WriteMatrix(output, new_body, new_head)

    def get_google_do_you_mean(self, input, output):    
        head, body = fio.ReadMatrix(input, hasHead=True)
        
        gse = GoogleSearchEngine()
        
        string_name_index = head.index('string name')
        
        cache_file = output + '.cache'
        if fio.IsExist(cache_file):
            cache = fio.LoadDict(cache_file, 'str')
        else:
            cache = {}
        
        string_names = [row[string_name_index] for row in body]
        
        new_head = ['google_do_you_mean'] + head
        new_body = []
        for i, string_name in enumerate(string_names):
            #print i, 
            if string_name in cache:
                hit = cache[string_name]
            else:
                try:
                    hit = gse.didYouMean(string_name)
                    cache[string_name] = hit
                    print i, string_name, hit
                except Exception as e:
                    fio.SaveDict(cache, cache_file)
             
            new_row = [hit] + body[i]
            new_body.append(new_row)
            
            if i%5 == 0:
                fio.SaveDict(cache, cache_file)
        
        fio.WriteMatrix(output, new_body, new_head)

    def get_canonical_string(self, input, output, db):    
        head, body = fio.ReadMatrix(input, hasHead=True)
        
        canonical_form_id_index = head.index('canonical_form_id')
        
        canonical_form_id = [row[canonical_form_id_index] for row in body]
        
        sql = 'select name from canonical_forms where id = '
        new_body = []
        new_head = ['canonical_name'] + head
        
        for i, id in enumerate(canonical_form_id):
            print i, 
            one_sql = sql + id
            
            if id != 'None':
                cursor = db.execute_sql(one_sql)
                N = cursor.rowcount
                
                if N == 0:
                    #row = ['None', 'None']
                    print id
                    continue
                elif N > 1:
                    print N, id
                    continue
                else:
                    row = cursor.fetchone()
                
            else:
                row = ['None']
            
            new_row = [row[0]] + body[i]
            new_body.append(new_row)
        
        fio.WriteMatrix(output, new_body, new_head)
    
    def get_feature(self, input, output, db):
        head, body = fio.ReadMatrix(input, hasHead=True)
        
        canonical_form_id_index = head.index('canonical_form_id')
        canonical_form_id = [row[canonical_form_id_index] for row in body]
        
        sql = 'select num_sources, source_nomenclatural_authority_total, source_nomenclatural_authority_max, source_taxonomic_authority_total, source_taxonomic_authority_max, has_path, match_path from canonical_forms_refinery where id = '
        
        new_head = ['num_sources', 
                    'source_nomenclatural_authority_total', 
                    'source_nomenclatural_authority_max', 
                    'source_taxonomic_authority_total', 
                    'source_taxonomic_authority_max', 
                    'has_path', 
                    'match_path'] + head
        new_body = []
        for i, id in enumerate(canonical_form_id):
            #print i, 
            one_sql = sql +  '"' + id + '"'
            
            if id != 'None':
                cursor = db.execute_sql(one_sql)
                N = cursor.rowcount
                
                if N == 0:
                    #row = ['None', 'None']
                    print id
                    continue
                elif N > 1:
                    print N, id
                    continue
                else:
                    row = cursor.fetchone()
            else:
                row = ['None', 'None', 'None', 'None', 'None', 'None', 'None',]
            
            new_row = [row[0], row[1], row[2], row[3], row[4], row[5], row[6]] + body[i]
            new_body.append(new_row)
        
        fio.WriteMatrix(output, new_body, new_head)
        
if __name__ == '__main__':
    import ConfigParser
    
    config = ConfigParser.RawConfigParser()
    config.read('../config/myconfig.cfg')
    
    host = config.get('mysql', 'host')
    user = config.get('mysql', 'user')
    passwd = config.get('mysql', 'passwd')
    db = config.get('mysql', 'db')
    
    datadir = config.get('dir', 'data')
    
    corpus = Corpus()
    
    import os
    corpus_folder = os.path.join(datadir, 'testset')
    corpus.load(corpus_folder)
    #corpus.normalize_label(corpus_folder + '/labels.txt')
    corpus.save(corpus_folder + '/v_combined.txt')
    
    #combined = corpus_folder + '/v_combined.txt'
    #output = corpus_folder + '/goodname_ratio_source.txt'
    #corpus.get_distribution(combined, output)
    
    #combined_with_cannonical_form = corpus_folder + '/v_combined_cannonical.txt'
    
    #db = GNI_DB(host=host, user=user, passwd=passwd, db=db)
    #corpus.get_cannonical_from(combined, combined_with_cannonical_form, db)
    
    #combined_with_cannonical_name = corpus_folder + '/v_combined_cannonical_name.txt'
    #corpus.get_canonical_string(combined_with_cannonical_form, combined_with_cannonical_name, db)
    
    #combined_with_cannonical_name_google = corpus_folder + '/combined_cannonical_name_google_doyoumean.txt'
    #combined_with_cannonical_name_feature = corpus_folder + '/v_combined_cannonical_name_feature.txt'
    #corpus.get_feature(combined_with_cannonical_name, combined_with_cannonical_name_feature, db)