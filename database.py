import sqlite3
import pandas as pd
# auto_00


class DB:

	def __init__(self):
		self.conn = sqlite3.connect('auto.db')
		self.cursor = self.conn.cursor()

	def csv2sql(self, filePath):
		df = pd.read_csv(filePath, encoding='utf-8')
		df.to_sql('auto_00', con=self.conn, if_exists='replace')


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
				end_dttm datetime default (datetime('2999-12-31 23:59:59'))
			);
			''')

		self.cursor.execute('''
			create view if not exists v_auto as
				select 
					id
					, auto_key
					, model
					, transmission
					, body_type
					, drive_type
					, color
					, production_year
					, engine_capacity
					, horsepower
					, engine_type
					, price
					, milage
				from auto 
				where current_timestamp between start_dttm and end_dttm

			;
			''')


	def createTableNewRows(self):
		# create auto_01
		self.cursor.execute('''
			create table auto_01 as 
				select
					t1.*
				from auto_00 t1
				left join v_auto t2
				on t1.auto_key = t2.auto_key
				where t2.auto_key is null;
			''')


	def createTableUpdateRows(self):
		# create auto_01
		self.cursor.execute('''
			create table auto_02 as 

				select 
					t1.*
				from auto_00 t1
				inner join v_auto t2
				on t1.auto_key = t2.auto_key
				and (
					t1.model              <> t2.model 
					or t1.transmission    <> t2.transmission
					or t1.body_type       <> t2.body_type
					or t1.drive_type      <> t2.drive_type
					or t1.color           <> t2.color
					or t1.production_year <> t2.production_year
					or t1.engine_capacity <> t2.engine_capacity
					or t1.horsepower      <> t2.horsepower
					or t1.engine_type     <> t2.engine_type
					or t1.price           <> t2.price
					or t1.milage          <> t2.milage
				);

			''')

	def createTableDeleteRows(self):
		self.cursor.execute('''
			create table auto_03 as 
				select
					t1.auto_key
				from v_auto t1
				left join auto_00 t2
				on t1.auto_key = t2.auto_key
				where t2.auto_key is null;
		''')

	def updateAutoTable(self):
		# логическое удаление старых записей
		self.cursor.execute('''
			update auto
			set end_dttm = current_timestamp
			where auto_key in (select auto_key from auto_03);
		''')
		
		# изменение записей
		self.cursor.execute('''
			update auto
			set end_dttm = current_timestamp
			where auto_key in (select auto_key from auto_02);
		''')

		self.cursor.execute('''
			insert into auto (
				auto_key,
				model,
				transmission,
				body_type,
				drive_type,
				color,
				production_year,
				engine_capacity,
				horsepower,
				engine_type,
				price,
				milage
			)
			select 
				auto_key,
				model,
				transmission,
				body_type,
				drive_type,
				color,
				production_year,
				engine_capacity,
				horsepower,
				engine_type,
				price,
				milage
			from auto_02;
		''')


		# добавление новых данных

		self.cursor.execute('''
			insert into auto (
				auto_key,
				model,
				transmission,
				body_type,
				drive_type,
				color,
				production_year,
				engine_capacity,
				horsepower,
				engine_type,
				price,
				milage
			)
			select 
				auto_key,
				model,
				transmission,
				body_type,
				drive_type,
				color,
				production_year,
				engine_capacity,
				horsepower,
				engine_type,
				price,
				milage
			from auto_01;
		''')


		self.conn.commit()

	def deleteTmpTables(self):
		self.cursor.execute('''
			drop table if exists auto_00;
		''')
		self.cursor.execute('''
			drop table if exists auto_01;
		''')
		self.cursor.execute('''
			drop table if exists auto_02;
		''')
		self.cursor.execute('''
			drop table if exists auto_03;
		''')



if __name__ == '__main__':
	fileURL = r'data2.csv'

	db = DB()
	db.deleteTmpTables()
	db.csv2sql(fileURL)
	db.createAutoTable()
	db.createTableNewRows()
	db.createTableUpdateRows()
	db.createTableDeleteRows()
	db.updateAutoTable()

	def readTable(tableName):
		sql = f'select * from {tableName}'
		db.cursor.execute(sql)
		return db.cursor.fetchall()


	print('_'*10 + 'auto_01' + '_'*10)

	for row in readTable('auto_01'):
		print(row)

	print('_'*10 + 'auto_02' + '_'*10)

	for row in readTable('auto_02'):
			print(row)

	print('_'*10 + 'auto_03' + '_'*10)

	for row in readTable('auto_03'):
		print(row)








