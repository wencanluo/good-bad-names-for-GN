import MySQLdb
from django.core.management.sql import sql_all

class GNI_DB:
	def __init__(self, host, user, passwd, db):
		self.db = MySQLdb.connect(host, user, passwd, db)
		
	def execute_sql(self, sql):
		cursor = self.db.cursor()
		
		try:
			cursor.execute(sql)
		except:
			print "Error: unable to execute sql: ", sql
		
		return cursor

	def get_canonical_forms(self, limit=1000):
		if limit != None:
			sql = "SELECT name FROM canonical_forms Limit " + str(limit)
		else:
			sql = "SELECT name FROM canonical_forms"

		try:
			# Execute the SQL command
			cursor = self.execute_sql(sql)
			N = cursor.rowcount
			for i in range(N):
				row = cursor.fetchone()
				
				yield row[0]
		except:
			print "Error: unable to fecth data"
	
	def get_canonical_forms_with_source_id(self, limit = 1000):
		start_n = 0
		n = 100
		
		sql = 'select canonical_forms.name, name_string_indices.data_source_id, name_string_indices.classification_path from name_strings join canonical_forms on name_strings.canonical_form_id = canonical_forms.id join name_string_indices on name_strings.id = name_string_indices.name_string_id '
						
		while True:
			print start_n
			
			if limit != None:
				cur_sql = sql + 'Limit ' + str(start_n) + ',' + str(n)
			
			print cur_sql
			
			try:
				# Execute the SQL command
				cursor = self.execute_sql(cur_sql)
				N = cursor.rowcount
				print 'N=', N
				
				if N == 0: break
				
				for i in range(N):
					row = cursor.fetchone()
					
					yield row
				
			except:
				print "Error: unable to fecth data"
				
			if limit != None and (start_n+n) > limit: break
			
			start_n = start_n + n
		
	def close(self):
		self.db.close()

if __name__ == '__main__':
	import ConfigParser
	
	config = ConfigParser.RawConfigParser()
	config.read('../config/myconfig.cfg')
	
	host = config.get('mysql', 'host')
	user = config.get('mysql', 'user')
	passwd = config.get('mysql', 'passwd')
	db = config.get('mysql', 'db')
	
	db = GNI_DB(host=host, user=user, passwd=passwd, db=db)
	names = db.get_canonical_forms_with_source_id(limit = 3000)
	
	for name in names:
		print name
	