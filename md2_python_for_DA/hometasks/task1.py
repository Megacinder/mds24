BASE_URL1 = (
    "http://learnonline.hse.ru/python-as-foreign/1/1.html",
    "http://learnonline.hse.ru/python-as-foreign/nonexistent",
)
BASE_URL2 = (
    "http://learnonline.hse.ru/python-as-foreign/1/1.html",
    "http://learnonline.hse.ru/python-as-foreign/nonexistent",
    "http://learnonline.hse.ru/python-as-foreign/2",
)
BASE_URL3 = "http://learnonline.hse.ru/python-as-foreign/tasks/pics/example.html"
BASE_URL4 = "https://learnonline.hse.ru/python-as-foreign/tasks/salary/example.html"
BASE_URL5 = "http://learnonline.hse.ru/python-as-foreign/tasks/music/nautilus.html"
BASE_URL6 = "https://learnonline.hse.ru/python-as-foreign/tasks/ps5/example/"
BASE_URL7 = "http://learnonline.hse.ru/python-as-foreign/tasks/dict/example.html"


def n1_site_health_check(url: str) -> str:
    from requests import get
    page = get(url)
    if page.status_code == 200:
        return "IT WORKS!"
    else:
        return "ERROR"


def n1_run():
    s = input()
    print(n1_site_health_check(s))


def n2_portfolio(urls: list) -> list:
    from bs4 import BeautifulSoup
    from requests import get

    titles = []
    for url in urls:
        page = get(url)
        page.encoding = 'utf-8'
        if page.status_code == 200:
            title = BeautifulSoup(page.text, "html.parser").title.string
            title_str = f'<a href="{url}">{title}</a>'
            titles.append(title_str)

    return titles


def n2_run():
    li1 = []
    while True:
        s = input()
        if s == 'конец':
            break
        li1.append(s)

    li2 = n2_portfolio(li1)
    print(*li2, sep='\n')


def n3_saved_images(url: str) -> list:
    from bs4 import BeautifulSoup
    from requests import get
    
    images = []

    page = get(url)
    page.encoding = 'utf-8'

    if page.status_code == 200:
        s = BeautifulSoup(page.text, "html.parser")  # .get_attribute_list('src')
        for i in s.find_all('img'):
            images.append(i['src'])

    return images


def n3_run():
    s = input()
    images = n3_saved_images(s)
    if not images:
        print("NO PICS FOUND")
    else:
        print(*images, sep='\n')


def n4_maximum_salary(url: str) -> dict:
    from bs4 import BeautifulSoup
    from requests import get
    
    li1, li2 = [], []

    page = get(url)
    page.encoding = 'utf-8'

    if page.status_code == 200:
        s = BeautifulSoup(page.text, "html.parser")  # .get_attribute_list('src')
        for i in s.find_all('td'):
            t = i.text
            if not t.isdigit():
                li1.append(t)
            else:
                li2.append(t)

    o = dict(zip(li1, li2))
    o1 = {k: v for k, v in sorted(o.items(), key=lambda i: int(i[1]), reverse=True)}
    o2 = list(o1.items())[0]
    o3 = f"{o2[0]}: {o2[1]} руб."
    return o3


def n4_run():
    s = input()
    s1 = n4_maximum_salary(s)
    print(s1)


def n5_this_music_will_last_forever(url: str) -> dict:
    from bs4 import BeautifulSoup
    from requests import get
    
    li1, li2 = [], []

    page = get(url)
    page.encoding = 'utf-8'

    if page.status_code == 200:
        s = BeautifulSoup(page.text, "html.parser")  # .get_attribute_list('src')
        for i in s.find_all('td'):
            t = i.text
            if not t.isdigit():
                li1.append(t)
            else:
                li2.append(t)

    o = dict(zip(li1, li2))
    o1 = {k: v for k, v in sorted(o.items(), key=lambda i: int(i[1]), reverse=True)}
    o2 = list(o1.items())[:3]
    # o3 = f"{o2[0]}: {o2[1]} руб."
    o3 = [i[0] for i in o2]
    return o3


def n5_run():
    s = input()
    s1 = n5_this_music_will_last_forever(s)
    print(*s1, sep='\n')


def n6_compare_prices_in_multiple_stores(url: str) -> dict:
    from bs4 import BeautifulSoup
    from requests import get

    di1 = {}

    page = get(url)
    page.encoding = 'utf-8'

    if page.status_code == 200:
        s = BeautifulSoup(page.text, "html.parser")
        for i in s.find_all('a'):
            url1 = url + i['href']
            page1 = get(url1)
            page1.encoding = 'utf-8'
            s1 = BeautifulSoup(page1.text, "html.parser")
            for k in s1.find_all('body'):
                s2 = k.text.strip()
                di1[url1] = [i for i in s2.split(' ') if i.isdigit()][0]

    o1 = {k: v for k, v in sorted(di1.items(), key=lambda i: int(i[1]))}
    o2 = list(o1.items())[0]
    o3 = f"{o2[0]}: {o2[1]} руб."
    return o3


def n6_run():
    s = input()
    s1 = n6_compare_prices_in_multiple_stores(s)
    print(s1)


def n7_translator(s1: str, s2: str, url: str) -> str:
    trans = parse_table(url)
    li1 = []
    s1 = s1.split(' ')
    for i in s1:
        for words in trans:
            if (i == words[0] and s2 == 'ENG,RUS') or (i == words[1] and s2 == 'RUS,ENG'):
                li1.append(words[1] if s2 == 'ENG,RUS' else words[0])
    o = ' '.join([i for i in li1 if i])

    return o


def n7_run():
    s1 = input()  # "пустынная роза"
    s2 = input()  # "RUS,ENG"
    url = input()  # "http://learnonline.hse.ru/python-as-foreign/tasks/dict/example.html"
    s = n7_translator(s1, s2, url)
    print(s)


def n8_list_of_movies(urls: list) -> list:
    table_list = []
    for url in urls:
        table = parse_table(url)
        table_list.append(table)

    movies = []
    for table in table_list:
        rat = max([i[3] for i in table])
        table = [i for i in table if i[3] == rat]
        table = sorted(table, key=lambda x: x[0])
        mov = [i[0] for i in table]
        for i in mov:
            if i not in movies:
                movies.append(i)

    return movies[:2]  # don't know why two movies are in the final only - should be three


def n8_run():
    urls = []
    while True:
        s1 = input()
        if s1 == 'конец':
            break
        urls.append(s1)
        
    s = n8_list_of_movies(urls)
    print(*s, sep='\n')


def n9_filmography(urls: list, d: str) -> list:
    table_list = []
    for url in urls:
        table = parse_table(url)
        table = [i for i in table if d == i[1]]
        table_list.append(table)

    movies = []
    for table in table_list:
        for i in table:
            if i not in movies:
                movies.append(i)            

    movies = sorted(
        movies,
        key=lambda x: (-1 * int(x[3]), x[0]),
    )

    movies = [f"{i[0]} ({i[4]})" for i in movies]

    return movies


def n9_run():
    urls = []
    s2 = ''
    while True:
        s1 = input()
        if not s1.startswith('http://'):
            s2 = s1
            break
        urls.append(s1)

    s = n9_filmography(urls, s2)
    print(*s, sep='\n')




from bs4 import BeautifulSoup, ResultSet


def get_soup(url: str) -> ResultSet:
    from requests import get

    page = get(url)
    page.encoding = 'utf-8'

    if page.status_code == 200:
        soup = BeautifulSoup(page.text, "html.parser")
        return soup
    return None


def parse_table(soup: ResultSet) -> list:
    a = list()

    table = soup.find('table')
    rows = table.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        if cols:
            cols1 = [i.text.strip() for i in cols]
            a.append(cols1)

    return a


def rec_get_table(tables: list, url: str, soup: ResultSet) -> tuple:
    try:
        table = parse_table(soup)
        tables.append(table)

        for s in soup.find_all('a'):
            if s.next_element == "Следующая таблица результатов":
                url1 = url + s['href']
                soup1 = get_soup(url1)
                tables, soup, url = rec_get_table(tables, url, soup1)
    except Exception as e:
        pass

    return tables, url1, soup


def filter_table(table: list, s1: str, s2: str) -> list:
    table = [i for i in table if s1.lower() in i[1].lower()]

    cols = {"TSS": 2, "TES": 3, "PCS": 4, "SS": 5, "TR": 6, "PE": 7, "CO": 8, "IN": 9}
    cols1 = {}
    c = s2.split(',')

    for i in c:
        cols1[i] = cols[i]

    ft = []
    for i in table:
        ft.append(i)

    ft2 = []
    col_set = []

    for row in table:
        for i in c:
            col_set.append(row[cols[i]])
        ft2.append(' '.join(col_set))
        col_set = []

    return ft2


def n10_figure_skating(url: str, s1: str, s2: str) -> list:
    soup = get_soup(url)
    tables = []
    tables, _, _ = rec_get_table(tables, url, soup)

    t = []
    for table in tables:
        for i in table:
            t.append(i)

    t = filter_table(t, s1, s2)
            
    return t


def n10_run():
    url = input()  # 'http://learnonline.hse.ru/python-as-foreign/tasks/fs/example/'
    s1 = input()  # 'Плисецкий'
    s2 = input()  # 'TSS,IN,TES'

    s = n10_figure_skating(url, s1, s2)
    print(*s, sep='\n')


if __name__ == '__main__':
    n10_run()
