import sqlite3


class DB:

	def __init__(self):
		self.conn = sqlite3.connect('auto.db')
		self.cursor = self.conn.cursor()

	def createAutoTable(self):
		self.cursor.execute('''
			create table if not exists auto(
				id integer primary key autoincrement,
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
				milage int
			);
			''')

	
