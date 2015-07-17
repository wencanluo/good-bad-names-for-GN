import fio
import os
from OrigReader import prData
from collections import defaultdict
import json
import codecs

class VerNetCorpus:
    def __init__(self, excelfile=None):
        self.excelfile = excelfile
        
        self.name_key = 'constructedscientificname'
        self.label_key = 'con-valid'
        self.source_key = 'validSource'
        self.others = ['validCanonical','sourceURL', 'con-auth','con-sg','con-enc']
        self.types = ['con-ms','con-sp','con-inf','con-ws','con-cap','con-rnk','con-authcap','con-hyb','con-cf','con-qu','con-ab','con-ex']
        self.parse_features = ['parsed', 'has_canonical', 'has_species', 'has_author', 'has_year', 'hybrid', 'parser_run', 'surrogate', 'has_ignored']
        self.other_feature_keys = ['NetiNeti', 'TaxonFinder', 'has_question_mark', 'all_caplitized']
        self.feature_keys = self.parse_features + self.other_feature_keys
        
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
    
    def extract_parser_features_from_string(self, data):
        parsed_data = data['scientificName']
        
        parsed = parsed_data['parsed']
        
        name_string = parsed_data['verbatim'] if 'verbatim' in parsed_data else ''
        
        has_canonical = 'canonical' in parsed_data
        has_species = 'details' in parsed_data and len(parsed_data['details']) > 0 and 'species' in parsed_data['details'][0]
        has_basionymAuthorTeam = has_species and 'basionymAuthorTeam' in parsed_data['details'][0]['species']
        has_author = has_basionymAuthorTeam and 'author' in parsed_data['details'][0]['species']['basionymAuthorTeam']
        has_year = has_basionymAuthorTeam and 'year' in parsed_data['details'][0]['species']['basionymAuthorTeam']
        hybrid =  parsed_data['hybrid']==True if 'hybrid' in parsed_data else 'unk'
        parse_run = parsed_data['parser_run'] if 'parser_run' in parsed_data else 'unk'
        surrogate = parsed_data['surrogate'] == True if 'surrogate' in parsed_data else 'unk' 
        has_ignored = 'details' in parsed_data and len(parsed_data['details']) > 0 and 'ignored' in parsed_data['details'][0]
        
        return name_string, parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored 
        
    def get_parse_features(self, parsedfile):
        
        names = self.get_names()
        
        head = ['parsed', 'has_canonical', 'has_species', 'has_author', 'has_year', 'hybrid', 'parser_run', 'surrogate', 'has_ignored']
        body = []
        for name, line in zip(names, codecs.open(parsedfile, 'r', 'utf-8')):
            data = json.loads(line.strip(), encoding = 'utf-8')
            name_string, parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored = self.extract_parser_features_from_string(data)
            row = [parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored]
            body.append(row)
            
        fio.WriteMatrix('log.txt', body, head)
    
    def write_feature(self, feature_keys = None, output=None):
        if feature_keys == None:
            feature_keys = self.feature_keys
        if output == None:
            output = '_'.join(feature_keys)
        
        head = feature_keys + ['@class@']
        labels = self.get_labels()
        
        body = self.get_columns(feature_keys)
        for i, row in enumerate(body):
            row.append(labels[i])
        types = ['Category']*len(head)
        
        fio.ArffWriter('../data/weka/' + output + '.arff', head, types, 'vernet', body)
    
    def test_feature(self):
        #self.write_feature(['NetiNeti'])
        #self.write_feature(['TaxonFinder'])
        #self.write_feature(['has_question_mark'])
        #self.write_feature(['all_caplitized'])
        #self.write_feature(['NetiNeti', 'TaxonFinder'])
        #self.write_feature(['NetiNeti', 'TaxonFinder', 'has_question_mark', 'all_caplitized'])
        self.write_feature(self.parse_features, output='parse')
        self.write_feature(self.feature_keys, output='parse_netiti_taxonfinder')
        
if __name__ == '__main__':
    import ConfigParser
    
    config = ConfigParser.RawConfigParser()
    config.read('../config/myconfig.cfg')
    
    datadir = config.get('dir', 'data')
    
    vernet_corpus = os.path.join(datadir, 'testset', 'VertNetTaxonomyTestSet_clean.xls')
    
    corpus = VerNetCorpus(vernet_corpus)
    
    #corpus.error_analysis_by_source()
    
    parsedfile = os.path.join(datadir, 'testset', 'vernet_parsed.json')
    #corpus.get_parse_features(parsedfile)
    
    corpus.test_feature()
    