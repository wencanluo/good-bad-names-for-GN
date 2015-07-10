import fio
import os
from OrigReader import prData

class VerNetCorpus:
    def __init__(self, excelfile):
        self.excelfile = excelfile
        
        self.name_key = 'constructedscientificname'
        self.label_key = 'con-valid'
        self.source_key = 'validSource'
        self.others = ['validCanonical','sourceURL', 'con-auth','con-sg','con-enc']
        self.types = ['con-ms','con-sp','con-inf','con-ws','con-cap','con-rnk','con-authcap','con-hyb','con-cf','con-qu','con-ab','con-ex']

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
    
    def get_column(self, key):
        values = []
        orig = prData(self.excelfile, 0)
        for k, inst in enumerate(orig._data):
            value = inst[self.name_key]
            values.append(value)
        return values
        
    def get_names(self):
        self.names = self.get_column(self.name_key)
        return self.names
        
    def get_sources(self):
        self.sources = self.get_column(self.source_key)
        return self.sources
    
    def get_labels(self):
        self.labels = self.get_column(self.label_key)
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

if __name__ == '__main__':
    import ConfigParser
    
    config = ConfigParser.RawConfigParser()
    config.read('../config/myconfig.cfg')
    
    datadir = config.get('dir', 'data')
    
    vernet_corpus = os.path.join(datadir, 'testset', 'VertNetTaxonomyTestSet_clean.xls')
    
    corpus = VerNetCorpus(vernet_corpus)
    
    corpus.error_analysis()