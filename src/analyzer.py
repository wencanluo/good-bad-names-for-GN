from gni import GNI_DB
from collections import defaultdict
import fio

class Analyzer:
	def __init__(self, db):
		'''
		db is an instance of GNI_DB
		'''
		self.db = db
	
	def get_canonical_forms_length_distribution(self):
		'''
		get length distribution of the cannoical names in the data base
		return dict: (length: count)
		'''
		len_dis = defaultdict(int)
		
		names = self.db.get_canonical_forms(limit=None)
		
		for name in names:
			word_count = len(name.split())
			len_dis[word_count] += 1
		
		return len_dis
	
	def extract_canonical_forms_by_length(self, datadir):
		'''
		'''
		names = self.db.get_canonical_forms(limit=None)
		
		dict = {}
		for name in names:
			word_count = len(name.split())
			if word_count not in dict:
				dict[word_count] = []
			
			dict[word_count].append(name)
		
		for length in dict:
			fio.SaveList(dict[length], datadir + 'name_' + str(length) + '.txt')
	
	def extract_data_sources(self, output):
		dict = {}
		for row in self.db.get_data_sources():
			dict[row[0]] = row[1]
		fio.SaveDict(dict, output)
			
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
	
	analyzer = Analyzer(db)
	
	import time
	time_start = time.clock()
	#analyzer.extract_canonical_forms_by_length(datadir)
	import os
	data_source_json = os.path.join(datadir,'data_sources.dict')
	analyzer.extract_data_sources(data_source_json)
	
	time_analyzing = time.clock()
	print "analyzing time: %s" % (time_analyzing - time_start)
	
	