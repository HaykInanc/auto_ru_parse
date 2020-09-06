import sqlite3

# auto_00


class DB:

	def __init__(self):
		self.conn = sqlite3.connect('auto.db')
		self.cursor = self.conn.cursor()

	def createAutoTable(self):
		self.cursor.execute('''
			create table if not exists auto(
				id integer primary key autoincrement,
				auto_key integer,
				model varchar(128),
				transmission varchar(128),
				body_type varchar(128),
				drive_type varchar(128),
				color varchar(128),
				production_year int,
				engine_capacity real,
				horsepower int,
				engine_type varchar(128),
				price int,
				milage int,
				start_dttm datetime default current_timestamp,
				end_dttm datetime default datetime('2999-12-31 23:59:59')
				
			);
			''')


	def addNewRows(self):
		# create auto_01
		self.cursor.execute('''
			create table auto_01 as 
				select
					t1.*
				from auto_00 t1
				left join auto t2
				on t1.auto_key = t2.auto_key
				where t2.auto_key is null
			''')




