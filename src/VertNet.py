import file_util as fio
import os
from OrigReader import prData
from collections import defaultdict
import json
import codecs
import get_possible_miss_spelled_name_combine as google_dl
import numpy as np
import MySQLdb
from gni import GNI_DB

from algorithm import lcs

from get_parser_data import format_genus_species
from gn_parser import Parser

class VertNetCorpus:
    def __init__(self, excelfile=None):
        self.excelfile = excelfile
        self.parser = Parser()
        
        self.name_key = 'constructedscientificname'
        self.label_key = 'con-valid'
        self.source_key = 'validSource'
        self.others = ['validCanonical','sourceURL', 'con-auth','con-sg','con-enc']
        self.types = ['con-ms','con-sp','con-inf','con-ws','con-cap','con-rnk','con-authcap','con-hyb','con-cf','con-qu','con-ab','con-ex']
        self.parse_features = ['parsed', 'has_canonical', 'has_species', 'has_author', 'has_year', 'hybrid', 'parser_run', 'surrogate', 'has_ignored']
        self.parse_feature_types = ['Category']*len(self.parse_features)
        
        self.miss_spelling_features      = ['genus_freq', 'species_freq','genus_species_freq','PMI',        'genus_prob', 'species_prob','genus_google','genus_dl', 'genus_both','species_dl']
        self.miss_spelling_feature_types = ['Continuous', 'Continuous',  'Continuous',        'Continuous', 'Continuous', 'Continuous',  'Category',    'Category', 'Category',  'Category']
        
        self.authorship_features =     ['year_match', 'year_not_match', 'author_match', 'author_not_match', 'year_match_bigger', 'author_match_bigger']
        self.authorship_feature_types = ['Continuous', 'Continuous',     'Continuous',   'Continuous',       'Category',          'Category']
        
        self.classification_features =      ['has_classification_path', 'synonym',      'data_sources']
        self.classification_feature_types = ['Category',                'Category',     'Continuous']
        
        self.simple_features = ['has_question_mark', 'all_caplitized']
        self.simple_feature_types = ['Category']*len(self.simple_features)
        
        self.other_features = ['NetiNeti', 'TaxonFinder']
        self.other_feature_types = ['Category']*len(self.other_features)
        
        self.feature_keys = self.other_features + self.simple_features + self.parse_features +  self.miss_spelling_features + self.authorship_features + self.classification_features
        self.feature_types = self.other_feature_types +self.simple_feature_types + self.parse_feature_types +  self.miss_spelling_feature_types + self.authorship_feature_types + self.classification_feature_types
        
    def load(self):
        orig = prData(self.excelfile, 0)
        name_key = 'constructedscientificname'
        label_key = 'con-valid'
        others = ['validCanonical','validSource','sourceURL', 'con-auth','con-sg','con-enc']
        types = ['con-ms','con-sp','con-inf','con-ws','con-cap','con-rnk','con-authcap','con-hyb','con-cf','con-qu','con-ab','con-ex']
        
        for k, inst in enumerate(orig._data):
            label = inst[label_key]
            name = inst[name_key]
            #print label
            
            for key in types:
                mark = inst[key]
                if type(mark) == int:
                    #print mark
                    if label == 'valid': print key, label, name
    
    def get_columns(self, keys):
        body = []
        orig = prData(self.excelfile, 0)
        for k, inst in enumerate(orig._data):
            row = []
            for key in keys:
                value = inst[key]
                if type(value) == str:
                    value = value.rstrip()
                row.append(value)
            body.append(row)
        return body
    
    def get_column(self, key):
        values = []
        orig = prData(self.excelfile, 0)
        for k, inst in enumerate(orig._data):
            value = inst[key]
            values.append(value)
        return values
        
    def get_names(self):
        self.names = self.get_column(self.name_key)
        return self.names
        
    def get_sources(self):
        self.sources = self.get_column(self.source_key)
        return self.sources
    
    def get_labels(self):
        labels = self.get_column(self.label_key)
        self.labels = ['good' if label == 'valid' else 'bad' for label in labels]
        return self.labels
        
    def error_analysis(self):
        orig = prData(self.excelfile, 0)
        name_key = 'constructedscientificname'
        label_key = 'con-valid'
        others = ['validCanonical','validSource','sourceURL', 'con-auth','con-sg','con-enc']
        types = ['con-ms','con-sp','con-inf','con-ws','con-cap','con-rnk','con-authcap','con-hyb','con-cf','con-qu','con-ab','con-ex']
        
        dict = {}
        
        count = 0
        for k, inst in enumerate(orig._data):
            label = inst[label_key]
            if label != 'valid':
                count += 1
            name = inst[name_key]
            
            flag = False
            for key in types:
                mark = inst[key]
                if type(mark) == int:#A label as "TRUE"'
                    if key not in dict:
                        dict[key] = []
                    
                    flag = True
                    dict[key].append(name)
            
            if label != 'valid' and not flag:
                if 'others' not in dict:
                        dict['others'] = []
                dict['others'].append(name)
        
        for key in sorted(dict, key=dict.get):
            print "%s\t%d\t%s"%(key, len(dict[key]) , ', '.join(dict[key]))
                                  
        print len(orig._data)
        print count
    
    def error_analysis_by_source(self):
        orig = prData(self.excelfile, 0)
        
        dict = defaultdict(int)
        total_dict = defaultdict(int)
        
        for k, inst in enumerate(orig._data):
            label = inst[self.label_key]
            sources = inst[self.source_key]
            for source in sources.split('|'):
                source = source.strip()
                if len(source) == 0:
                    source = 'unknown'
                total_dict[source] += 1
            
                if label != 'valid':
                    dict[source] += 1
        
        for key in sorted(dict, key=dict.get, reverse = True):
            print key, '\t', total_dict[key], '\t', dict[key], '\t', float(dict[key])/total_dict[key]
    
    def has_question_mark(self, name):
        if '?' in name:
            return True
        return False
    
    def all_caplitized(self, name):
        for word in name.split():
            if len(word) >= 3 and word.isupper():
                return True
        
        return False
       
    def extract_features(self):
        names = self.get_names()
        
        f_fun = [self.has_question_mark, self.all_caplitized]
        
        f_vec = []
        for name in names:
            row = []
            for f in f_fun:
                row.append(f(name))
            f_vec.append(row)
            
        fio.WriteMatrix('log.txt', f_vec, header=None)
    
    def get_parse_features(self, parsedfile):
        names = self.get_names()
        
        head = ['parsed', 'has_canonical', 'has_species', 'has_author', 'has_year', 'hybrid', 'parser_run', 'surrogate', 'has_ignored']
        body = []
        for name, line in zip(names, codecs.open(parsedfile, 'r', 'utf-8')):
            data = line.strip()
            name_string, parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored = self.extract_parser_features_from_string(data)
            row = [parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored]
            body.append(row)
            
        fio.WriteMatrix('log.txt', body, head)
    
    def rank2prob(self, r, R):
        return 1 - np.log(r)/np.log(R)
    
    def get_canonical_names(self, parsedfile, output):
        names = self.get_names()
        
        dict = {}
        for name, line in zip(names, codecs.open(parsedfile, 'r', 'utf-8')):
            for info in self.parser.extract_info(line.strip()):
                if info == None: continue
                
                canonical = info['canonical']
                
                dict[name] = canonical
        
        with codecs.open(output, 'w', 'utf-8') as fout:
            json.dump(dict, fout, indent=2)
        
    def get_misspelling_features(self, parsedfile, genus_file, species_file, genus_species_file, 
                                 genus_suggested_by_google, genus_suggested_by_dl, genus_suggested_by_both,
                                 species_suggested_by_dl,
                                 output
                                 ):
        
        names = self.get_names()
        
        dict_genus_freq = google_dl.get_dict(genus_file)
        print "total genus: %d" %len(dict_genus_freq)
        
        dict_species_freq = google_dl.get_dict(species_file)
        print "total species: %d" %len(dict_species_freq)
        
        dict_genus_species_freq = google_dl.get_dict(genus_species_file)
        print "total genus_species: %d" %len(dict_genus_species_freq)
        
        dict_genus_rank = google_dl.get_rank_dict(genus_file)
        dict_species_rank = google_dl.get_rank_dict(species_file)
        
        R_genus = len(dict_genus_rank)
        R_species = len(dict_species_rank)
        
        dict_genus_suggested_by_google = google_dl.get_dict(genus_suggested_by_google)
        dict_genus_suggested_by_dl = google_dl.get_dict(genus_suggested_by_dl)
        dict_genus_suggested_by_both = google_dl.get_dict(genus_suggested_by_both)
        dict_species_suggested_by_dl = google_dl.get_dict(species_suggested_by_dl)
        
        keys = ['genus', 'species']
        
        head = ['genus_freq', 'species_freq', 'genus_species_freq', 'PMI', 'genus_prob', 'species_prob', 'genus_google', 'genus_dl', 'genus_both', 'species_dl']
        body = []
        
        name_count = 0
        for name, line in zip(names, codecs.open(parsedfile, 'r', 'utf-8')):
            for info in self.parser.extract_info(line.strip()):
                if info == None: continue
                
                name_count += 1
                
                genus, species = [info[key] for key in keys]
                
                genus_freq = int(dict_genus_freq[genus]) if genus != None and genus in dict_genus_freq else 0
                species_freq = int(dict_species_freq[species]) if species != None and species in dict_species_freq else 0
                
                if genus != None and species != None:
                    genus_species = format_genus_species(genus, species)
                    genus_species_freq = int(dict_genus_species_freq[genus_species]) if genus_species in dict_genus_species_freq else 0
                    if genus_freq > 0 and species_freq > 0 and genus_species_freq > 0:
                        PMI = np.log2(float(genus_species_freq)/(float(genus_freq)*species_freq))
                    else:
                        PMI = 0
                else:
                    genus_species_freq = 0
                    PMI = 0
                
                r_genus = dict_genus_rank[genus] if genus != None and genus in dict_genus_rank else R_genus
                r_species = dict_species_rank[species] if species != None and species in dict_species_rank else R_species
                
                genus_prob = self.rank2prob(r_genus, R_genus)
                species_prob  = self.rank2prob(r_species, R_species)
                
                genus_google = genus in dict_genus_suggested_by_google and dict_genus_suggested_by_google[genus] != None
                genus_dl = genus in dict_genus_suggested_by_dl and dict_genus_suggested_by_dl[genus] != None
                genus_both = genus in dict_genus_suggested_by_both and dict_genus_suggested_by_both[genus] != None
                species_dl = species in dict_species_suggested_by_dl and dict_species_suggested_by_dl[species] != None
                
            row = [genus_freq, species_freq, genus_species_freq, PMI, genus_prob, species_prob, genus_google, genus_dl, genus_both, species_dl]
                
            body.append(row)
            
            #if name_count > 1000:
            #    break
        fio.WriteMatrix(output, body, head)
    
    def check_year(self, year, database):
        match = 0
        not_match = 0
                    
        for k, v in database.items():
            dict = json.loads(k)
            
            if year == dict['year']:
                match += v
            else:
                not_match += v
        
        return match, not_match    
    
    def author_match_lcs(self, authors1, authors2):
        strAuthors1 = ' '.join(sorted(authors1))
        strAuthors2 = ' '.join(sorted(authors2))
        
        if authors1 == authors2: return True
        
        if len(strAuthors1) < 2: return False
        if len(strAuthors2) < 2: return False
        
        m_lcs = lcs(strAuthors1, strAuthors2)
        if len(m_lcs) >= min(len(strAuthors1)/2, len(strAuthors2)/2):
            return True
        return False
        
    def check_author(self, authors, database):
        match = 0
        not_match = 0
                    
        for k, v in database.items():
            dict = json.loads(k)
            
            if self.author_match_lcs(authors, dict['authors']):
                match += v
            else:
                not_match += v
        
        return match, not_match   
        
    def get_authorship_features(self, parsedfile, canonical_database, output):
        with codecs.open(canonical_database, 'r', 'utf-8') as fin:
            dict_canonical_database = json.load(fin)
        
        names = self.get_names()
        
        head = ['year_match', 'year_not_match', 'author_match', 'author_not_match', 'year_match_bigger', 'author_match_bigger']
        body = []
        
        keys = ['authors', 'year', 'canonical']
        for name, line in zip(names, codecs.open(parsedfile, 'r', 'utf-8')):
            for info in self.parser.extract_info(line.strip()):
                if info == None: continue
                
                authors, year, canonical = [info[key] for key in keys]
                
                if authors == None or year == None or canonical == None:
                    year_match = -1
                    year_not_match = -1
                    author_match = -1
                    author_not_match = -1
                    year_match_bigger = 0
                    author_match_bigger = 0
                    continue

                if canonical in dict_canonical_database:
                    year_match, year_not_match = self.check_year(year, dict_canonical_database[canonical])
                    author_match, author_not_match = self.check_author(authors, dict_canonical_database[canonical])
                else:
                    year_match = -1
                    year_not_match = -1
                    author_match = -1
                    author_not_match = -1
                
                if year_match > year_not_match:
                    year_match_bigger = 1
                elif year_match < year_not_match:
                    year_match_bigger = -1
                else:
                    year_match_bigger = 0
                
                if author_match > author_not_match:
                    author_match_bigger = 1
                elif author_match < author_not_match:
                    author_match_bigger = -1
                else:
                    author_match_bigger = 0
                                    
            row = [year_match, year_not_match, author_match, author_not_match, year_match_bigger, author_match_bigger]
            body.append(row)
            
        fio.WriteMatrix(output, body, head)
    
    def get_name_ids(self, output):
        vertnet_names_index = '../data/vertnet_names_index.txt'
        header, body = fio.ReadMatrix(vertnet_names_index, hasHead=True)
        
        all_name_strings_ids = '../data/all_name_strings.id_list'
        ids = fio.ReadFile(all_name_strings_ids)
        
        data = {}
        for row in body:
            index, name = row
            
            data[name.rstrip()] = ids[int(index.rstrip())].rstrip(' \r\n')
        
        with codecs.open(output, 'w', 'utf-8') as fout:
            json.dump(data, fout, indent=2)
            
    def get_classification_path_feature(self, db, vertnet_name_ids, output):
        with codecs.open(vertnet_name_ids, 'r', 'utf-8') as fin:
            data = json.load(fin)
        
        head = ['has_classification_path', 'synonym', 'data_sources']
        body = []
        for name in self.get_names():
            if name in data:
                name_id = int(data[name])
                
                has_classification_path, synonym, data_sources = db.get_classification_features_from_id(name_id)
                
                if has_classification_path == None: has_classification_path = 0
                if synonym == None: synonym = 0
                
                len_source = 0
                if data_sources == None: 
                    len_source = 0
                else:
                    len_source = len(data_sources.split(','))
                
                row = [has_classification_path, synonym, len_source]
                
            else:
                row = [0, 0, 0]
            
            body.append(row)
        
        fio.WriteMatrix(output, body, head)
            
    def write_feature(self, feature_keys = None, feature_types = None, output=None):
        if feature_keys == None:
            feature_keys = self.feature_keys
        if output == None:
            output = '_'.join(feature_keys)
        if feature_types == None:
            feature_types = ['Category']*len(feature_keys)
        
        head = feature_keys + ['@class@']
        labels = self.get_labels()
        
        body = self.get_columns(feature_keys)
        for i, row in enumerate(body):
            row.append(labels[i])
        types = feature_types + ['Category']
        
        fio.ArffWriter('../data/weka/' + output + '.arff', head, types, 'vernet', body)
    
    def test_feature(self):
        #self.write_feature(['NetiNeti'])
        #self.write_feature(['TaxonFinder'])
        #self.write_feature(['has_question_mark'])
        #self.write_feature(['all_caplitized'])
        #self.write_feature(['NetiNeti', 'TaxonFinder'])
        #self.write_feature(['NetiNeti', 'TaxonFinder', 'has_question_mark', 'all_caplitized'])
        #self.write_feature(self.parse_features, output='parse')
        
        #self.write_feature(self.miss_spelling_features, self.miss_spelling_feature_types, output='misspelling')
        #self.write_feature(self.feature_keys, self.feature_types, output='parse_netiti_taxonfinder_misspelling')
        
        #self.write_feature(self.classification_features, self.classification_feature_types, output='classification')
        self.write_feature(self.feature_keys, self.feature_types, output='parse_netiti_taxonfinder_misspelling_authorship_classification_gn')
        
        #self.write_feature(self.classification_features + self.parse_features, 
        #                   self.classification_feature_types + self.parse_feature_types,
        #                   output= 'classification_parser'
        #                   )
        
if __name__ == '__main__':
    import ConfigParser
    
    config = ConfigParser.RawConfigParser()
    config.read('../config/myconfig.cfg')
    
    datadir = config.get('dir', 'data')
    
    vernet_corpus = os.path.join(datadir, 'testset', 'VertNetTaxonomyTestSet_clean.xls')
    
    corpus = VertNetCorpus(vernet_corpus)
    
#     #corpus.error_analysis_by_source()
#     
#     parsedfile = os.path.join(datadir, 'testset', 'vernet_parsed.json')
#     #corpus.get_parse_features(parsedfile)
#     
#     genus_file = os.path.join(datadir, 'genus.txt')
#     species_file = os.path.join(datadir, 'species.txt')
#     genus_species_file = os.path.join(datadir, 'info_genus_species.txt')
#     
#     genus_suggested_by_google = os.path.join(datadir, 'genus_suggested_by_google.txt')
#     genus_suggested_by_dl = os.path.join(datadir, 'genus_suggested_by_dl.txt')
#     genus_suggested_by_both = os.path.join(datadir, 'genus_suggested_by_both.txt')
#     species_suggested_by_dl = os.path.join(datadir, 'species_suggested_by_dl.txt')
#                                  
#     output = os.path.join(datadir, 'misspelling_log.txt')
# #     corpus.get_misspelling_features(parsedfile, genus_file, species_file, genus_species_file, 
# #                                  genus_suggested_by_google, genus_suggested_by_dl, genus_suggested_by_both,
# #                                  species_suggested_by_dl,
# #                                  output
# #                                  )
#     
#     
#     vernet_canonical = os.path.join(datadir, 'vernet_canonical.json')
#     #corpus.get_canonical_names(parsedfile, output)
#     
#     canonical_database = os.path.join(datadir, 'same_name_but_different_author_vertnet.json')
#     
#     authorship_features = os.path.join(datadir, 'authorship_features.txt')
#     #corpus.get_authorship_features(parsedfile, canonical_database, authorship_features)
#     
#     id_file = os.path.join(datadir, 'all_name_strings.id_list')
#     all_name_strings = os.path.join(datadir, 'all_name_strings.txt')
#     
#     vertnet_name_ids = os.path.join(datadir, 'vertnet_names_ids.txt')
#     #corpus.get_name_ids(vertnet_name_ids)
#     
#     host = config.get('mysql', 'host')
#     user = config.get('mysql', 'user')
#     passwd = config.get('mysql', 'passwd')
#     db = config.get('mysql', 'db')
#     db = GNI_DB(host=host, user=user, passwd=passwd, db=db)
#     
    #vertnet_classfication_feature = os.path.join(datadir, 'vertnet_classfication_feature.txt')
    #corpus.get_classification_path_feature(db, vertnet_name_ids, vertnet_classfication_feature)
    
    corpus.test_feature()