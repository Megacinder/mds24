from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from requests import get

BASE_URL = 'https://learnonline.hse.ru/python-as-foreign/1/10.html'

page = get(BASE_URL)
# page.encoding = 'utf-8'
# print(page.encoding)
# print(page.content.decode())
# print(page.content)
# soup = BeautifulSoup(page.content, "html.parser")
# links = soup.find_all('a')
# for link in links:
#     print(link)
    # if link.has_attr('href'):
    #     print(link['href'])

page = get('https://online.hse.ru/python-as-foreign/1333/')
print(page.status_code)



# def get_page_len(link):
#     page = get(BASE_URL + link)
#     if page.status_code == 200:
#         page.encoding = 'utf-8'
#         s = BeautifulSoup(page.text, "html.parser")
#         print(f'The article length is {len(s.find("body").text)} symbols.')
#         s.encode()


# link_list = [
#     link.get('href')
#     for link in soup.find_all('a')
#     if link.get('href').endswith('html')
# ]

# with ThreadPoolExecutor() as executor:
#     executor.map(get_page_len, link_list)
