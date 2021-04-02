import json
import os
import requests


class Parse5ka:

    __5ka_api_url = 'https://5ka.ru/api/v2'
    __categories_api_url = f'{__5ka_api_url}/categories/'
    __products_api_url = f'{__5ka_api_url}/special_offers/'
    __output_directory = 'data'

    def __init__(self, records_per_page=15):
        self.records_per_page = records_per_page

    def __get_products(self, category_id):
        url = self.__products_api_url
        items = []
        params = {
            'categories': category_id,
            'records_per_page': self.records_per_page
        }
        while url is not None:
            response = requests.get(url, params=params)
            json_data = response.json()
            if json_data['results']:
                items.extend(json_data['results'])
                url = json_data['next']
            else:
                url = None
        return items

    def parse(self):
        response = requests.get(self.__categories_api_url)
        for category in response.json():
            category_id = category['parent_group_code']
            data = {
                'name': category['parent_group_name'],
                'code': category_id,
                'products': self.__get_products(category_id)
            }
            self.__save_to_file(category_id, data)

    def __save_to_file(self, category_id, data):
        if not os.path.exists(self.__output_directory):
            os.makedirs(self.__output_directory)
        with open(os.path.join('data/', f'{category_id}.json'), 'w') as outfile:
            json.dump(data, outfile, indent=4, sort_keys=True)


Parse5ka().parse()
