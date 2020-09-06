import requests
from bs4 import BeautifulSoup
import csv
import subprocess
import os
from datetime import datetime


def __clear_data(path):
	subprocess.call(['spark-submit', 'clear_data.py', path])


def __append_row(path, row):
	with open(path, 'a', encoding='utf8') as file:
		writer = csv.writer(file)
		writer.writerow(row)




def __parse_page(url, mark, page_num):

	result = requests.get(url.format(mark, page_num))
	soup = BeautifulSoup(result.content, 'html.parser')
	resultCarsList = []
	carsList = soup.select('.ListingItem-module__container')
	if len(carsList) == 0:
		return -1


	for car in carsList:
		resultrow = []
		try:
			resultrow.append(car.select_one('.ListingItemTitle-module__link').text)

			for option in car.select('.ListingItemTechSummaryDesktop__cell'):
				resultrow.append(option.text)

			resultrow.append(car.select_one('.ListingItemPrice-module__content').text)
			resultrow.append(car.select_one('.ListingItem-module__year').text)
			resultrow.append(car.select_one('.ListingItem-module__kmAge').text)
			link = car.select_one('.Link.ListingItemTitle-module__link').get('href')
			resultrow.append(link.split('/')[-2].split('-')[0])
			resultCarsList.append(resultrow)
		except Exception:
			pass

	return resultCarsList


def get_data(mark, filePath):

	url = 'https://auto.ru/moskva/cars/{}/all/?page={}&output_type=list'
	page_num = 30


	while True:

		result = __parse_page(url, mark, page_num)
		if result == -1:
			break
		else: 
			for row in result:
				__append_row(filePath, row)
		
		print(str(page_num) + '='*20)
		page_num += 1



if __name__ == '__main__':

	dirname = r'results/' + datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
	os.mkdir(dirname)


	get_data('honda', dirname+'/result.csv')
	__clear_data(dirname)

