import MySQLdb

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
			sql = "SELECT * FROM canonical_forms Limit " + str(limit)
		else:
			sql = "SELECT * FROM canonical_forms"

		try:
			# Execute the SQL command
			cursor = self.execute_sql(sql)
			N = cursor.rowcount
			for i in range(N):
				row = cursor.fetchone()
				
				yield row
		except:
			print "Error: unable to fecth data"
			
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
	names = db.get_canonical_forms()
	
	for name in names:
		print name
	