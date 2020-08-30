from flask import Flask 
import json
from parse import get_data

app = Flask(__name__)


@app.route('/<name>')

def main(name):
	return f'hello {name}'



@app.route('/api/get_data_by_mark/<mark>')

def parse_data(mark):
	try:
		get_data(mark, 'result.csv')
		return json.dumps({"status":'ok'})
	except Exception as e:
		return json.dumps({"status":'err', 'error_text': str(e)})
	





if __name__ == '__main__':
	app.debug = True
	app.run()