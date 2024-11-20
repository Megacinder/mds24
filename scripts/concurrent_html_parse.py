from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from requests import get

BASE_URL = 'https://online.hse.ru/python-as-foreign/1/'

page = get(BASE_URL)
page.encoding = 'utf-8'
soup = BeautifulSoup(page.text, "html.parser")


def get_page_len(link):
    page = get(BASE_URL + link)
    if page.status_code == 200:
        page.encoding = 'utf-8'
        s = BeautifulSoup(page.text, "html.parser")
        print(f'The article length is {len(s.find("body").text)} symbols.')


link_list = [
    link.get('href')
    for link in soup.find_all('a')
    if link.get('href').endswith('html')
]

with ThreadPoolExecutor() as executor:
    executor.map(get_page_len, link_list)
