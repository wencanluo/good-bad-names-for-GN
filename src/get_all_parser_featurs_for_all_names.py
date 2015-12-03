from warnings import filterwarnings
import MySQLdb
filterwarnings('ignore', category = MySQLdb.Warning)

import file_util as fio
import os
from OrigReader import prData
from collections import defaultdict
import json
import codecs
import get_possible_miss_spelled_name_combine as google_dl
import numpy as np
from gn_parser import Parser
from VertNet import VertNetCorpus
from gni import GNI_DB

import itertools as it
from algorithm import lcs

from get_parser_data import format_genus_species
from gn_parser import Parser

debug = False

def generator_all_name_strings(parser_output):
    line_count = 0
    for line in codecs.open(parser_output, 'r', 'utf-8'):
        row = line.split('\t')
        
        try:
            [id, name_string, parsed, has_canonical, has_species, has_author, has_year, hybrid, parse_run, surrogate, has_ignored] = row
            
            yield name_string
        except ValueError:
            continue
    
def get_misspelling_authorship_features(db, id_file, genus_file, species_file, genus_species_file, 
                                 genus_suggested_by_google, genus_suggested_by_dl, genus_suggested_by_both,
                                 species_suggested_by_dl,
                                 canonical_database,
                                 output
                                 ):
        
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
        
        with codecs.open(canonical_database, 'r', 'utf-8') as fin:
            dict_canonical_database = json.load(fin)
            
        keys1 = ['genus', 'species']
        keys2 = ['authors', 'year', 'canonical']
        
        head1 = ['genus_freq', 'species_freq', 'genus_species_freq', 'PMI', 'genus_prob', 'species_prob', 'genus_google', 'genus_dl', 'genus_both', 'species_dl']
        head2 = ['year_match', 'year_not_match', 'author_match', 'author_not_match', 'year_match_bigger', 'author_match_bigger']
        
        parser = Parser()
        vertnet = VertNetCorpus()
        
        import sys
        oldstdout = sys.stdout
        sys.stdout = codecs.open(output, 'w', 'utf-8')
        
        name_count = 0
        for row in db.get_parsed_name_strings(limit = None):
            id, data = row
            for info in parser.extract_info(data):
                if info == None: continue
            
                name_count += 1
                
                genus, species = [info[key] for key in keys1]
                
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
                
                genus_prob = vertnet.rank2prob(r_genus, R_genus)
                species_prob  = vertnet.rank2prob(r_species, R_species)
                
                genus_google = genus in dict_genus_suggested_by_google and dict_genus_suggested_by_google[genus] != None
                genus_dl = genus in dict_genus_suggested_by_dl and dict_genus_suggested_by_dl[genus] != None
                genus_both = genus in dict_genus_suggested_by_both and dict_genus_suggested_by_both[genus] != None
                species_dl = species in dict_species_suggested_by_dl and dict_species_suggested_by_dl[species] != None
                
                authors, year, canonical = [info[key] for key in keys2]
                
                if authors == None or year == None or canonical == None:
                    year_match = -1
                    year_not_match = -1
                    author_match = -1
                    author_not_match = -1
                    year_match_bigger = 0
                    author_match_bigger = 0
                    continue

                if canonical in dict_canonical_database:
                    year_match, year_not_match = vertnet.check_year(year, dict_canonical_database[canonical])
                    author_match, author_not_match = vertnet.check_author(authors, dict_canonical_database[canonical])
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
            
            row1 = [genus_freq, species_freq, genus_species_freq, PMI, genus_prob, species_prob, genus_google, genus_dl, genus_both, species_dl]
            row2 = [year_match, year_not_match, author_match, author_not_match, year_match_bigger, author_match_bigger]

            fio.PrintListSimple([id] + row1 + row2)
                                
        sys.stdout = oldstdout

def insert_to_db(db, misspelling_authorship_feature_file):
    
    keys = ['id', 
            'genus_freq', 'species_freq', 'genus_species_freq', 'PMI', 'genus_prob', 'species_prob', 'genus_google', 'genus_dl', 'genus_google_dl', 'species_dl', 
            'year_match', 'year_not_match', 'author_match', 'author_not_match', 'year_match_bigger', 'author_match_bigger'
            ]
    
    count = 0
    for i, line in enumerate(codecs.open(misspelling_authorship_feature_file, 'r', 'utf-8')):
        tokens = line.rstrip('\r\n').split('\t')
        
        dict = {}
        for k, key in enumerate(keys):
            value = tokens[k].strip()
            
            if value == 'True':
                value = 1
            elif value == 'False':
                value = 0
            
            dict[key] = value
        
        for k, v in dict.items(): #for sql
            dict[k] = str(v)
        
        sql = 'INSERT INTO name_string_refinery (' + ','.join(dict.keys()) + ') VALUES (' +','.join(dict.values())+ ')'
        if debug:
            print sql
        
        try:
            cursor = db.execute_sql(sql)
            N = cursor.rowcount
        except Exception as e:
            print e
            print dict
            print "Error: unable to insert data"
        
        if i%10000 == 0:
            print i
            db.commit()
        
        count += 1
        
        if debug:
            if count > 10: break 
        
    db.commit()
    
    print "total count:", count
            
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
    
    genus_file = os.path.join(datadir, 'genus.txt')
    species_file = os.path.join(datadir, 'species.txt')
    genus_species_file = os.path.join(datadir, 'info_genus_species.txt')
    
    genus_suggested_by_google = os.path.join(datadir, 'genus_suggested_by_google.txt')
    genus_suggested_by_dl = os.path.join(datadir, 'genus_suggested_by_dl.txt')
    genus_suggested_by_both = os.path.join(datadir, 'genus_suggested_by_both.txt')
    species_suggested_by_dl = os.path.join(datadir, 'species_suggested_by_dl.txt')
    
    canonical_database = os.path.join(datadir, 'same_name_but_different_author_all.json')
    
    id_file = os.path.join(datadir, 'all_name_strings_id.txt')
    
    output = os.path.join(datadir, 'all_name_strings.misspelling_authorship')
    get_misspelling_authorship_features(db, id_file, genus_file, species_file, genus_species_file, 
                                 genus_suggested_by_google, genus_suggested_by_dl, genus_suggested_by_both,
                                 species_suggested_by_dl,
                                 canonical_database,
                                 output
                                 )
    insert_to_db(db, output)
    