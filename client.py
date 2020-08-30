import requests

result = requests.get('http://localhost:5000/api/get_data_by_mark/uaz')

print(result.text)


