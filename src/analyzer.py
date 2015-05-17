from gni import GNI_DB
from collections import defaultdict

class Analyzer:
	def __init__(self, db):
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
		
if __name__ == '__main__':
	import ConfigParser
	
	config = ConfigParser.RawConfigParser()
	config.read('../config/myconfig.cfg')
	
	host = config.get('mysql', 'host')
	user = config.get('mysql', 'user')
	passwd = config.get('mysql', 'passwd')
	db = config.get('mysql', 'db')
	
	db = GNI_DB(host=host, user=user, passwd=passwd, db=db)
	
	analyzer = Analyzer(db)
	
	import time
	time_start = time.clock()
	len_dis = analyzer.get_canonical_forms_length_distribution()
	time_analyzing = time.clock()
	print "analyzing time: %s" % (time_analyzing - time_start)
	
	for i in range(1, max(len_dis.keys())+1):
		print i, '\t', len_dis[i]
	