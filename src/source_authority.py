import json

class SourceAuthority:
    def __init__(self, source_authority):
        self.authority = {}
        
        for i, line in enumerate(open(source_authority)):
            tokens = line.rstrip('\r\n').split('\t')
            if i==0: 
                head = tokens
                continue
            
            id = tokens[0]
            dict = {}
            
            nomenclatural_authority = tokens[2].strip()
            if len(nomenclatural_authority) == 0:
                dict['nomenclatural_authority'] = None
            else:
                dict['nomenclatural_authority'] = int(nomenclatural_authority)
                
            taxonomic_authority = tokens[4]
            if len(taxonomic_authority) == 0:
                dict['taxonomic_authority'] = None
            else:
                dict['taxonomic_authority'] = int(taxonomic_authority)
                
            self.authority[id] = dict
    
    def get_nomenclatural_authority_total(self, sources):
        value = 0
        for source in sources:
            if source in self.authority:
                v = self.authority[source]['nomenclatural_authority']
                if v != None:
                    value += v
        return value
    
    def get_nomenclatural_authority_max(self, sources):
        value = 0
        for source in sources:
            if source in self.authority:
                v = self.authority[source]['nomenclatural_authority']
                if v != None and v > value:
                    value = v
        return value
    
    def get_nomenclatural_authority_ave(self, sources):
        value = 0
        count = 0
        for source in sources:
            if source in self.authority:
                v = self.authority[source]['nomenclatural_authority']
                if v != None:
                    value += v
                    count += 1
        return float(value)/count
        
    def get_taxonomic_authority_total(self, sources):
        value = 0
        for source in sources:
            if source in self.authority:
                v = self.authority[source]['taxonomic_authority']
                if v != None:
                    value += v
        return value
    
    def get_taxonomic_authority_max(self, sources):
        value = 0
        for source in sources:
            if source in self.authority:
                v = self.authority[source]['taxonomic_authority']
                if v != None and v > value:
                    value = v
        return value
    
    def get_taxonomic_authority_ave(self, sources):
        value = 0
        count = 0
        for source in sources:
            if source in self.authority:
                v = self.authority[source]['taxonomic_authority']
                if v != None:
                    value += v
                    count += 1
        return float(value)/count