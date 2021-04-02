import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import dateparser
from time import sleep
import sys


class ParserMagnit:
    __magnit_url = 'https://magnit.ru'

    def __init__(self, mongo_client, db_name, collection_name):
        self.__db_name = db_name
        self.__collection_name = collection_name
        db = mongo_client[self.__db_name]
        self.__collection = db[self.__collection_name]

    def parse(self):
        print('Making request...')
        soup = BeautifulSoup(requests.get(f'{self.__magnit_url}/promo/?geo=moskva').text, 'lxml')
        print('Parsing...')
        items = soup.find_all(class_='card-sale')
        total = len(items)
        for i, item in enumerate(items):
            self.__progressbar(i, total)
            sleep(0.05)
            self.__parse_item(item)
        print(
            f'\nResult successfully saved to : \n\tDatabase = {self.__db_name}\n\tCollection = {self.__collection_name}')

    def __parse_item(self, item):
        product = {}
        for key, parser in self.__get_parse_dict().items():
            product[key] = parser(item)
        self.__collection.insert_one(product)

    def __get_parse_dict(self):
        return {
            'url': lambda tag: f'{self.__magnit_url}{tag["href"]}',
            'promo_name': lambda tag: self.__get_text_inside_paragraph(tag, 'card-sale__header'),
            'product_name': lambda tag: self.__get_text_inside_paragraph(tag, 'card-sale__title'),
            'old_price': lambda tag: self.__get_price(tag, True),
            'new_price': lambda tag: self.__get_price(tag),
            'image_url': lambda tag: f'{self.__magnit_url}{tag.find("img", {"class": "lazy"})["data-src"]}',
            'date_from': lambda tag: self.__get_date(tag),
            'date_to': lambda tag: self.__get_date(tag, False),
            'shop_type': lambda tag: self.__get_shop_type(tag)
        }

    def __get_shop_type(self, tag):
        footer_block = self.__get_div_by_class(tag, 'card-sale__footer')
        type_block = self.__get_div_by_class(footer_block, 'card-sale__type')
        icon_item = [c for c in type_block.find('span').attrs['class'] if '_icon_' in c][0]
        return icon_item.split('_')[-1]

    def __get_text_inside_paragraph(self, tag, css_class):
        div_block = self.__get_div_by_class(tag, css_class)
        return div_block.find('p').text if div_block else None

    @staticmethod
    def __get_div_by_class(tag, css_class):
        return tag.find('div', {'class': css_class})

    @staticmethod
    def __get_price(tag, is_old_price=False):
        price_block = tag.find('div', {'class': f'label__price label__price_{"old" if is_old_price else "new"}'})

        try:
            price_parts = list(map(lambda item: item.text, price_block.find_all('span') if price_block else []))
            return float('.'.join(price_parts)) if price_parts else 0
        except ValueError:
            return 0

    @staticmethod
    def __get_date(tag, is_from=True):
        dates_list = list(
            map(lambda item: item.text.split(' ', 1)[1],
                tag.find('div', {'class': 'card-sale__date'}).find_all('p')))
        return dateparser.parse(dates_list[0] if is_from else dates_list[1], languages=['ru']).timestamp()

    def __progressbar(self, i, total):
        n_bar = 10
        j = i / total
        sys.stdout.write('\r')
        sys.stdout.write(f"[{'=' * int(n_bar * j):{n_bar}s}] {int(100 * j)}%")
        sys.stdout.flush()
        if i == total - 1:
            self.__progressbar(total, total)


ParserMagnit(MongoClient('mongodb://localhost:27017/'), 'task2', 'magnit_collection').parse()
