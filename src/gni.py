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
	
	def get_data_sources(self):
		sql = "SELECT id, title FROM data_sources"

		try:
			# Execute the SQL command
			cursor = self.execute_sql(sql)
			N = cursor.rowcount
			for i in range(N):
				row = cursor.fetchone()
				
				yield row
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
		
	def get_name_string_indices(self, limit=1000):
		size = 10000
		last_id = 0
		
		while True:
			sql = 'SELECT name_string_id, data_source_id, classification_path, taxon_id, accepted_taxon_id, synonym FROM name_string_indices WHERE name_string_id > '+str(last_id)+' ORDER BY name_string_id LIMIT '+str(size)
			
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
	
	def get_all_features_from_name_string_refinery(self, limit=1000):
		size = 10000
		last_id = 0
		
		while True:
			sql = 'SELECT * FROM name_string_refinery WHERE id > '+str(last_id)+' ORDER BY id LIMIT '+str(size)
			
			try:
				cursor = self.execute_sql(sql)
				N = cursor.rowcount
				if N==0:break
				if limit != None and last_id >= limit: break
				
				for i in range(N):
					row = cursor.fetchone()
					last_id = row[0]
					yield row
			except Exception as e:
				print e
				print "Error: unable to fecth data"
	
	def get_name_source(self, limit=1000):
		size = 10000
		last_id = 0
		
		while True:
			sql = 'SELECT id, synonym, data_sources, good_bad, comment FROM name_string_refinery WHERE id > '+str(last_id)+' ORDER BY id LIMIT '+str(size)
			
			try:
				cursor = self.execute_sql(sql)
				N = cursor.rowcount
				if N==0:break
				if limit != None and last_id >= limit: break
				
				for i in range(N):
					row = cursor.fetchone()
					last_id = row[0]
					yield row
			except Exception as e:
				print e
				print "Error: unable to fecth data"	
		
	def get_bad_names(self, limit=1000):
		size = 10000
		last_id = 0
		
		while True:
			sql = 'SELECT id, synonym, data_sources, comment FROM name_string_refinery WHERE id > '+str(last_id)+' AND (good_bad = 0) ORDER BY id LIMIT '+str(size)
			
			try:
				cursor = self.execute_sql(sql)
				N = cursor.rowcount
				if N==0:break
				if limit != None and last_id >= limit: break
				
				for i in range(N):
					row = cursor.fetchone()
					last_id = row[0]
					yield row
			except Exception as e:
				print e
				print "Error: unable to fecth data"	
		
	def get_simple_badname_ids(self, limit=1000):
		size = 10000
		last_id = 0
		
		while True:
			sql = 'SELECT id, comment FROM name_string_refinery WHERE id > '+str(last_id)+' AND (has_question_mark = 1 OR parsed = 0 OR hybrid=1 OR surrogate=1 OR has_ignored=1) ORDER BY id LIMIT '+str(size)
			
			try:
				cursor = self.execute_sql(sql)
				N = cursor.rowcount
				if N==0:break
				if limit != None and last_id >= limit: break
				
				for i in range(N):
					row = cursor.fetchone()
					last_id = row[0]
					yield row
			except Exception as e:
				print e
				print "Error: unable to fecth data"	
	
	def get_goodbad_label(self, id):
		sql = 'SELECT good_bad, comment FROM name_string_refinery WHERE id = %d' % id
		try:
			cursor = self.execute_sql(sql)
			N = cursor.rowcount
			if N==0: return None
			
			row = cursor.fetchone()
			
		except Exception as e:
			print e
			print "Error: unable to fecth data"
			row = None
			
		return row
	
	def update_table(self, table_name, id, features, keys):
		assert(len(features) == len(keys))
		
		key_feature_pair = []
		for key, feature in zip(keys, features):
			key_feature_pair.append('='.join([key, feature]))

		sql = 'UPDATE %s SET %s WHERE id = %d' % (table_name, ','.join(key_feature_pair), id)

		try:
			cursor = self.execute_sql(sql)
			N = cursor.rowcount
		except Exception as e:
			print e
			print dict
			print "Error: unable to insert data"
	
	def get_name_string_from_id(self, id):
		sql = 'SELECT name FROM name_strings WHERE id = %d' % (id)
		try:
			cursor = self.execute_sql(sql)
			N = cursor.rowcount
			if N==0: return None
			
			row = cursor.fetchone()
			name = row[0]
		except Exception as e:
			print e
			print "Error: unable to fecth data"	
			name = None
			
		return name
	
	def get_source_name_from_id(self, id):
		sql = 'SELECT title FROM data_sources WHERE id = %d' % (id)
		try:
			cursor = self.execute_sql(sql)
			N = cursor.rowcount
			if N==0: return None
			
			row = cursor.fetchone()
			name = row[0]
		except Exception as e:
			print e
			print "Error: unable to fecth data"	
			name = None
			
		return name
	
	
	def get_classification_features_from_id(self, id):
		sql = 'SELECT has_classification_path, synonym, data_sources FROM name_string_refinery WHERE id = %d' % (id)
		try:
			cursor = self.execute_sql(sql)
			N = cursor.rowcount
			if N==0: return None
			
			row = cursor.fetchone()
			
		except Exception as e:
			print e
			print "Error: unable to fecth data"	
			row = None
			
		return row
	
	def add_empty_feature_id(self, feature_name, limit=1000):
		#the first columne of the row should be id
		size = 1000
		last_id = 0
		
		while True:
			sql = 'SELECT id FROM name_string_refinery WHERE '+feature_name+' is NULL AND id > '+str(last_id)+' ORDER BY id LIMIT '+str(size)
			
			try:
				cursor = self.execute_sql(sql)
				N = cursor.rowcount
				if N==0:break
				if limit != None and last_id >= limit: break
				
				for i in range(N):
					row = cursor.fetchone()
					last_id = row[0]
					yield row
			except Exception as e:
				print e
				print "Error: unable to fecth data"	
	
	def get_parsed_name_strings(self, limit=1000):
		size = 10000
		last_id = 0
		
		while True:
			sql = 'SELECT id, data FROM parsed_name_strings WHERE id > '+str(last_id)+' ORDER BY id LIMIT '+str(size)
			
			try:
				cursor = self.execute_sql(sql)
				N = cursor.rowcount
				if N==0:break
				if limit != None and last_id >= limit: break
				
				for i in range(N):
					row = cursor.fetchone()
					last_id = row[0]
					yield row
			except Exception as e:
				print e
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
	