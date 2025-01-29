1. Если винда или мак, то надо сказать докет десктоп: https://www.docker.com/products/docker-desktop/ (в маке можно через манагер пакетов brew)

(в линуксе он по умолчанию уже есть, насколько я помню - если нет, то тоже качаем - тоже через манагер пакетов apt, yum и т.д. - и ставим)

Докер - программулина для работы с контейнерами (это что-то типа виртуалки - когда у тебя создаётся изолированная копия операционной системы)

2. Качаем бобра https://dbeaver.io/, версию коммунити - это ИДЕ для работы с кучей СУБД (систем управления базами данных)

2. Запускаем докер (он жрёт дофига памяти)

3. Создаём где-то файл docker-compose.yml и пишем туда:
```
services:
  postgres:
    container_name: postgres1
    image: postgres:16.4-alpine  #версию можно выбрать другую; alpine - самый лёгкий по объёму линух
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: admin
      POSTGRES_DB: db
    ports:
      - 5432:5432
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: [ "CMD", "pg_isready", "-U", "db" ]
      interval: 5s
      retries: 5
    restart: always

volumes:
  postgres_data:
```

4. Через терминал мака/линуха или cmd винды переходим в папку с файлом из п.3 и запускаем:
docker compose -f docker-compose.yml up -d
флаг "-d", если не хотим видеть всей простыни, которой срёт докер-образ

5. Запускаем dbeaver, в левом верхнем углу нажимаем кнопку "new database connection", выбираем PostgreSQL (значок синей башки слона) - он там потом качает драйвера, всё такое.

6. Все данные для подключения оставляем по умолчанию, а юзер и пароль такие:
username = admin
password = admin

7. Ну и мы на сервере. Далее можно создать базу и какую-нить табличку:
