import sqlite3


class DB:

	def __init__(self):
		self.conn = sqlite3.connect('auto.db')
		cursor = conn.cursor()

	