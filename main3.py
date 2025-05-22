import time
import random

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


BASE_URL4 = "https://ru1.rp5.ru/download/files.metar/KX/KXNA.01.05.2025.21.05.2025.1.0.0.en.utf8.00000000.csv.gz"

options = Options()
options.add_argument("user-agent=" + UserAgent().random)
driver = webdriver.Chrome(options=options)
driver.get(BASE_URL4)
time.sleep(5)  # Ждём загрузки