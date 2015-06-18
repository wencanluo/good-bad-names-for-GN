import MySQLdb
import MySQLdb.cursors
from django.core.management.sql import sql_all

class GNI_DB:
	def __init__(self, host, user, passwd, db):
		#self.db = MySQLdb.connect(host, user, passwd, db, cursorclass = MySQLdb.cursors.SSCursor)
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
	
	def get_canonical_forms_with_id(self, limit=1000):
		sql = "SELECT id, name FROM canonical_forms ORDER BY id"
		
		if limit != None:
			sql +=  " Limit " + str(limit)
		
		try:
			# Execute the SQL command
			cursor = self.execute_sql(sql)
			N = cursor.rowcount
			for i in range(N):
				row = cursor.fetchone()
				
				yield row
		except:
			print "Error: unable to fecth data"
	
	def get_name_string_indices_old(self, limit=1000):
		sql = "SELECT data_source_id, name_string_id FROM name_string_indices ORDER BY name_string_id"
		
		if limit != None:
			sql +=  " Limit " + str(limit)
		
		try:
			# Execute the SQL command
			cursor = self.execute_sql(sql)
			N = cursor.rowcount
			
			for i in range(N):
				row = cursor.fetchone()
				
				yield row
		except:
			print "Error: unable to fecth data"
	
	def get_name_string_indices(self, limit=1000):
		size = 10000
		last_id = 0
		
		while True:
			sql = 'SELECT name_string_id, data_source_id, classification_path FROM name_string_indices WHERE name_string_id > '+str(last_id)+' ORDER BY name_string_id LIMIT '+str(size)
			
			try:
				cursor = self.execute_sql(sql)
				N = cursor.rowcount
				if N==0:break
				if limit != None and last_id >= limit: break
				
				for i in range(N):
					row = cursor.fetchone()
					last_id = row[0]
					yield row
			except:
				print "Error: unable to fecth data"	

	def get_name_string(self, limit=1000):
		size = 10000
		last_id = 0
		
		while True:
			sql = 'SELECT id, canonical_form_id FROM name_strings WHERE id > '+str(last_id)+' ORDER BY id LIMIT '+str(size)
			
			try:
				cursor = self.execute_sql(sql)
				N = cursor.rowcount
				if N==0:break
				if limit != None and last_id >= limit: break
				
				for i in range(N):
					row = cursor.fetchone()
					last_id = row[0]
					yield row
			except:
				print "Error: unable to fecth data"	
										
	def get_canonical_forms_with_source_id(self, limit = 1000):
		'''
		This doesn't work
		'''
		size = 10000
		last_id = 0
						
		while True:
			sql = 'select canonical_forms.name, name_string_indices.data_source_id, name_string_indices.classification_path, name_string_indices.name_string_id from name_strings join canonical_forms on name_strings.canonical_form_id = canonical_forms.id join name_string_indices on name_strings.id = name_string_indices.name_string_id WHERE name_string_indices.name_string_id > '+str(last_id)+' ORDER BY name_string_indices.name_string_id LIMIT '+str(size)
			
			try:
				cursor = self.execute_sql(sql)
				N = cursor.rowcount
				if N==0:break
				if limit != None and last_id >= limit: break
				
				for i in range(N):
					row = cursor.fetchone()
					last_id = row[3]
					yield row
			except:
				print "Error: unable to fecth data"	
	
	def add_source_authority(self, source_authority):
		for i, line in enumerate(open(source_authority)):
			tokens = line.strip().split('\t')
			if i==0: 
				head = tokens
				continue
			
			dict = {}
			
			for k, v in zip(head, tokens):
				if len(v) != 0:
					if k=='id' or k=='nomenclatural_authority' or k=='taxonomic_authority':
						dict[k] = v
					else:
						dict[k] = '"'+ v + '"'
			
			#insert it to the data base
			sql = 'INSERT INTO data_sources_authority (' + ','.join(dict.keys()) + ') VALUES (' +','.join(dict.values())+ ')'
			print sql
			
			try:
				cursor = self.execute_sql(sql)
				N = cursor.rowcount
				print N
			except:
				print "Error: unable to insert data"
				
		self.commit()
	
	def commit(self):
		self.db.commit()
		
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
	
	datadir = config.get('dir', 'data')
	
	import os
	source_authority = os.path.join(datadir, config.get('knowledge', 'source_authority'))
	
	db = GNI_DB(host=host, user=user, passwd=passwd, db=db)
	db.add_source_authority(source_authority)
	
	#names = db.get_canonical_forms_with_source_id(limit = 3000)
	
	#for name in names:
	#	print name
	