## Общее
Сервис по сбору и хранению ставок по кредитам и вкладам с banki.ru
## Описание проекта
На данный момент представлен ETL-пайплайн для сбора данных ставок по кредитам и вкладам, доступным в 100 городах РФ. Данные собираются с агрегатора banki.ru и после обработки передаются на удаленную БД.
## Архитектура / схема работы
Структура проекта:
```bash
├── *config.py*
├── etl
│   ├── *db_writer.py*
│   ├── *extract.py*
│   ├── load.py
│   ├── *transform.py*
│   ├── *transform_all.py*
├── ***main.py***
├── parsers
│   ├── *credits_parser_b.py*
│   ├── *deposits_parser_b.py*
│   ├── *regions_parser_b.py*
├── *requirements.txt*
├── storage
│   ├── data_raw
│   ├── history
├── utils
```
Файл ***main.py*** запускает процесс сбора данных - функцию /etl/extract.py/collect_new_data, которая вначале собирает данные об актуальных регионах и их идентификаторов, после чего для каждого региона запускает *credits_parser_b.py* и *deposits_parser_b.py*. Для каждого региона данные на этом этапе сохраняются в
```bash
── data_raw
│   ├── credits
│   │   ├── 103
│   │   │   ├── banki_credits_dump_2025-11-17.csv
│   │   │   └── banki_credits_dump__2025-11-16.csv
│   ├── deposits
│   │   ├── 103
│   │   │   ├── banki_deposits_dump_2025-11-17.csv
│   │   │   └── banki_deposits_dump__2025-11-16.csv
```
Т.е. в /storage/data_raw/{credits or deposits}/{region_id}/banki_{credits or deposits}_ {timestamp}.csv
После сбора в main.py запускается функция transform_all(), которая создает единую базу предложений, добавляет хэш поле для каждого, актуализирует данные через [SCD type 2]([Версионность и история данных / Хабр](https://habr.com/ru/articles/101544/?ysclid=mi51d6rc11997408535)) и делает дополнительные преобразования данных. После чего имеем 5 файлов в /storage/history
```bash
├── credits_products_history.csv
├── credits_regions_history.csv
├── deposits_products_history.csv
├── deposits_regions_history.csv
└── regions.csv
```
Первые два - набор всех существовавших (и существующих) предложений, 3 и 4 - таблицы регион-предложения (один кредит может быть доступен для нескольких регионов и несколько кредитов - для одного региона). В завершении этапа transform_all запускает функцию cleanup_raw(), удаляющую все raw файлы старше 3х дней (все данные с них уже перенесены в /history и бд, потребность в них отпадает).

Далее в main.py вызывается DBWriter().run_all() - подключение к удаленной БД (используем [Neon](https://console.neon.tech/)) и сохранение данных в 6 таблиц: banks, regions, credit_products, credit_regions, deposit_products, deposit_regions. Ознакомиться подробнее со столбцами и типами значений в них можно в etl/db_writer.py (def \_\_init__())

## Установка и запуск
```python
pip install -r requirements.txt
python main.py
```
Также необходимы .env переменные, импортируемые в программу: данные подключения к базе данных и куки.
## Как работает GitHub Actions
GH actions запускает main.py ежедневно в 13:00 по мск, по выполнении - пушит новые файлы в репозиторий и отправляет в neon db. Время работы зависит от скорости парсинга (частое падение подключения к сайту приводит к долгим паузам) и в среднем занимает от 60-100 минут.