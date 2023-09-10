iimport requests
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import decimal
from time import localtime, strftime
from datetime import datetime

# Создаю словари значений из данных, предоставляемых API, для USD
# Создаю DAG для USD
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1)
}

USD_variables = Variable.set(key="currency_load_variables",
                         USD_url_conf = {
                            "USD_symbol": "symbol",
                            "USD_open": "open",
                            "USD_high:": "high",
                            "USD_low": "low",
                            "USD_price": "price",
                            "USD_volume": "volume",
                            "USD_latest trading day": "latest trading day",
                            "USD_previous close": "previous close",
                            "USD_change": "change",
                            "USD_change percent": "change percent",
                            "USD_url_base": "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=USD"
                                            "&apikey=WEOQ7XCN7FTX2I4Z",
                                            "connection_name": "my_db_conn"},
                         serialize_json = True)
USD_dag_variables = Variable.get("currency_load_variables", deserialize_json=True)

def import_codes(**kwargs):
    url = USD_dag_variables.get('USD_url_base')

    try:
        response = requests.get(url, params={'base': USD_dag_variables.get('USD_url_base')})
    except Exception as err:
        print(f'Error occured: {err}')
        return

    data = response.json()

with DAG(dag_id = "calc-rates", schedule_interval = "*/10 * * * *",
    default_args = default_args, tag=["USD"], catchup = False) as dag:

    USD_get_bash_task = BashOperator(task_id = 'bash_task', bash_command = "Get USD")

# Создаю словари значений из данных, предоставляемых API, для CNY
# Создаю DAG для CNY

CNY_variables = Variable.set(key="currency_load_variables",
                             CNY_url_conf={
                                 "CNY_symbol": "symbol",
                                 "CNY_open": "open",
                                 "CNY_high:": "high",
                                 "CNY_low": "low",
                                 "CNY_price": "price",
                                 "CNY_volume": "volume",
                                 "CNY_latest trading day": "latest trading day",
                                 "CNY_previous close": "previous close",
                                 "CNY_change": "change",
                                 "CNY_change percent": "change percent",
                                 "CNY_url_base":"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=CNY"
                                                "&apikey=WEOQ7XCN7FTX2I4Z",
                                 "connection_name": "my_db_conn"},
                         serialize_json = True)
CNY_dag_variables = Variable.get("currency_load_variables", deserialize_json=True)

def import_codes(**kwargs):
    url = CNY_dag_variables.get('CNY_url_base')

    try:
        response = requests.get(url, params={'base': CNY_dag_variables.get('CNY_url_base')})
    except Exception as err:
        print(f'Error occured: {err}')
        return

    data = response.json()

with DAG(dag_id = "calc-rates", schedule_interval = "*/10 * * * *",
    default_args = default_args, tag=["CNY"], catchup = False) as dag:


    CNY_get_bash_task = BashOperator(task_id = 'bash_task', bash_command = "Get CNY")

# Создаю словари значений из данных, предоставляемых API, для IBM
# Создаю DAG для IBM
CNY_variables = Variable.set(key="currency_load_variables",
                             IBM_url_conf = {
                                "IBM_symbol": "symbol",
                                "IBM_open": "open",
                                "IBM_high:": "high",
                                "IBM_low": "low",
                                "IBM_price": "price",
                                "IBM_volume": "volume",
                                "IBM_latest trading day": "latest trading day",
                                "IBM_previous close": "previous close",
                                "IBM_change": "change",
                                "IBM_change percent": "change percent",
                                "IBM_url_base":
                                    "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=IBM&apikey"
                                    "=WEOQ7XCN7FTX2I4Z",
                                 "connection_name": "my_db_conn"},
                         serialize_json = True)
IBM_dag_variables = Variable.get("currency_load_variables", deserialize_json=True)

def import_codes(**kwargs):
    url = IBM_dag_variables.get('IBM_url_base')

    try:
        response = requests.get(url, params={'base': IBM_dag_variables.get('IBM_url_base')})
    except Exception as err:
        print(f'Error occured: {err}')
        return

    data = response.json()

with DAG(dag_id = "calc-rates", schedule_interval = "*/10 * * * *",
    default_args = default_args, tag=["IBM"], catchup = False) as dag:


    IBM_get_bash_task = BashOperator(task_id = 'bash_task', bash_command = "Get IBM")

# Инициирую подключение к БД Postgres
import psycopg2
from psycopg2 import Error

host = 'localhost'
port = '5432'
username = 'postgres'
password = 'test'
database = 'postgres'

try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        database=database
    )
    cur = conn.cursor()
    print("Successfully connected to the database")

# Создаю SQL - запросы
# Создаю, заполняю таблицу валют с первичным ключом id
    create_table_currencies_query = '''CREATE TABLE if not exist currencies (id SERIAL PRIMARY KEY,
        symbol CHAR NOT NULL)'''

    insert_table_currencies_query = '''INSERT INTO currencies (symbol)
    VALUES
    (USD),
    (CNY),
    (IBM)'''

# Создаю, заполняю таблицу данными для USD, связываю с таблицей валют по вторичному ключу id
    create_table_USD_kurs_query = '''CREATE TABLE if not exist
    USD_kurs (id FOREIGN KEY (id) REFERENCES currencies (id), symbol CHAR NOT NULL, open FLOAT NOT
    NULL, high FLOAT NOT NULL, low FLOAT NOT NULL, price FLOAT NOT NULL, volume INT NOT NULL, latest trading day DATE
    NOT NULL, previous close FLOAT NOT NULL, change FLOAT NOT NULL, change percent FLOAT NOT NULL)'''

    insert_table_USD_kurs_query = '''INSERT INTO
    USD_kurs (symbol, open, high, low, price, volume, latest trading day, previous close, change, change percent)
    VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (USD_url_conf["USD_symbol"], USD_url_conf["USD_open"], USD_url_conf[
        "USD_high"], USD_url_conf["USD_low"], USD_url_conf["USD_price"], USD_url_conf["USD_volume"], USD_url_conf[
        "USD_latest trading day"], USD_url_conf["USD_previous close"], USD_url_conf["USD_change"], USD_url_conf[
        "USD_change percent"])

# Создаю, заполняю таблицу данными для СNY, связываю с таблицей валют по вторичному ключу id
    create_table_CNY_kurs_query = '''CREATE TABLE if not exist
        CNY_kurs (id FOREIGN KEY (id) REFERENCES currencies (id), symbol CHAR NOT NULL, open FLOAT NOT
        NULL, high FLOAT NOT NULL, low FLOAT NOT NULL, price FLOAT NOT NULL, volume INT NOT NULL, latest trading day DATE
        NOT NULL, previous close FLOAT NOT NULL, change FLOAT NOT NULL, change percent FLOAT NOT NULL)'''

    insert_table_CNY_kurs_query = '''INSERT INTO
        CNY_kurs (symbol, open, high, low, price, volume, latest trading day, previous close, change, change percent)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (
    CNY_url_conf["CNY_symbol"], CNY_url_conf["CNY_open"], CNY_url_conf[
        "CNY_high"], CNY_url_conf["CNY_low"], CNY_url_conf["CNY_price"], CNY_url_conf["CNY_volume"], CNY_url_conf[
        "CNY_latest trading day"], CNY_url_conf["CNY_previous close"], CNY_url_conf["CNY_change"], CNY_url_conf[
        "CNY_change percent"])

# Создаю, заполняю таблицу данными для IBM, связываю с таблицей валют по вторичному ключу id
    create_table_IBM_kurs_query = '''CREATE TABLE if not exist
            IBM_kurs (id FOREIGN KEY (id) REFERENCES currencies (id), symbol CHAR NOT NULL, open FLOAT NOT
            NULL, high FLOAT NOT NULL, low FLOAT NOT NULL, price FLOAT NOT NULL, volume INT NOT NULL, latest trading day DATE
            NOT NULL, previous close FLOAT NOT NULL, change FLOAT NOT NULL, change percent FLOAT NOT NULL)'''

    insert_table_IBM_kurs_query = '''INSERT INTO
            IBM_kurs (symbol, open, high, low, price, volume, latest trading day, previous close, change, 
            change percent)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (
        IBM_url_conf["IBM_symbol"], IBM_url_conf["IBM_open"], IBM_url_conf[
            "IBM_high"], IBM_url_conf["IBM_low"], IBM_url_conf["IBM_price"], IBM_url_conf["IBM_volume"], IBM_url_conf[
            "IBM_latest trading day"], IBM_url_conf["IBM_previous close"], IBM_url_conf["IBM_change"], IBM_url_conf[
            "IBM_change percent"])

# Формирую витрину данных для всех таблиц
    select_table_query = ('''SELECT * FROM currencies JOIN USD_kurs ON currencies.id = USD_kurs.id JOIN CNY_kurs ON
    currencies.id = CNY_kurs.id JOIN IBM_kurs ON currencies.id = IBM_kurs.id ''')

    cur.execute(create_table_currencies_query, insert_table_currencies_query, create_table_USD_kurs_query,
                insert_table_USD_kurs_query, create_table_CNY_kurs_query, insert_table_CNY_kurs_query,
                create_table_IBM_kurs_query, insert_table_IBM_kurs_query,
                select_table_query)
    vit_currencies_kurs = cur.fetchall()

# Вывожу данные из витрины
    print("Data on the exchange rate of BTC in RUB:")
    for row in vit_currencies_kurs:
        print('id = ', row[0])
        print('symbol = ', row[1])
        print('open = ', row[2])
        print('high = ', row[3])
        print('low = ', row[4])
        print('price = ', row[5])
        print('volume = ', row[6])
        print('latest trading day = ', row[7])
        print('previous close = ', row[8])
        print('change = ', row[9])
        print('change percent = ', row[10])
        print('previous close = ', row[11])

    conn.commit()
except (Exception, Error) as error:
    print('Oshibka pri rabote s PostgreSQL', error)

# Прекращаю соединение с БД
finally:
    if conn:
        cur.close()
        conn.close()
        print('Soedinenie s PostgreSQL zakrito')