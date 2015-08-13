import json

class Parser:
    def __init__(self):
        pass
    
    def extract_info(self, data_string):
        data = json.loads(data_string, encoding = 'utf-8')
        
        parsed_data = data['scientificName']
        
        parsed = parsed_data['parsed']
        
        dict = {}
        
        for key in ['genus', 'species', 'authors', 'year', 'canonical']:
            dict[key] = None
        
        if 'canonical' in parsed_data:
            dict['canonical'] = parsed_data['canonical']
            
        if 'details' in parsed_data: 
            for detail in parsed_data['details']:
                if 'genus' in detail:
                    if 'string' in detail['genus']:
                        genus = detail['genus']['string']
                        dict['genus'] = genus
                
                if 'species' in detail:
                    if 'string' in detail['species']:
                        species = detail['species']['string']
                        dict['species'] = species
                
                if 'species' in detail:
                    if 'basionymAuthorTeam' in detail['species']:
                        if 'author' in detail['species']['basionymAuthorTeam']:
                            authors = detail['species']['basionymAuthorTeam']['author']
                            dict['authors'] = authors

                if 'species' in detail:
                    if 'basionymAuthorTeam' in detail['species']:
                        if 'year' in detail['species']['basionymAuthorTeam']:
                            year = detail['species']['basionymAuthorTeam']['year']
                            dict['year'] = year
                                               
        yield dict
          
    def extract_parser_features_from_string(self, data_string):
        data = json.loads(data_string, encoding = 'utf-8')
        
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
  