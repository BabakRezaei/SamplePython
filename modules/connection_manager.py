import pandas as pd
import os
from dotenv import load_dotenv
import psycopg2
import tempfile
from sqlalchemy import create_engine


def read_sql_tmpfile(query, db_engine, encoding):
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
            query=query, head="HEADER"
        )
        conn = db_engine.raw_connection()
        cur = conn.cursor()
        cur.copy_expert(copy_sql, tmpfile)
        tmpfile.seek(0)
        df = pd.read_csv(tmpfile, encoding=encoding)

        cur.close()
        conn.close()
        return df


def query_builder(table_name, condition_str, column_list):
    query = f'SELECT '

    if column_list is None:
        query = query + f'* FROM public.'
    else:
        if len(column_list) == 1:
            query = query + f'"{column_list[0]}"' + f' FROM public.'
        else:
            query = query + f'"{column_list[0]}"'
            for i in range(1, len(column_list)):
                query = query + f', ' + f'"{column_list[i]}"'
            query = query + f' FROM public.'
    tblName = f'"{table_name}"'  # f'{tblName}'
    query = query + tblName  # [2:-2]

    if condition_str is not None:
        query = query + f' ' + condition_str

    return query


def read_from_db(table_name, condition_str=None, column_list=None, encoding="UTF-8"):
    load_dotenv()

    DATABASE = os.getenv("DATABASE")
    USERNAME = os.getenv("DB_USERNAME")
    PASSWORD = os.getenv("DB_PASSWORD")
    DB_SERVER = os.getenv("DB_SERVER")
    DB_PORT = int(os.getenv("DB_PORT"))

    postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(
        username=USERNAME,
        password=PASSWORD,
        ipaddress=DB_SERVER,
        port=DB_PORT,
        dbname=DATABASE))

    engine = create_engine(postgres_str)

    query = query_builder(table_name=table_name, column_list=column_list, condition_str=condition_str)

    df = read_sql_tmpfile(query, engine, encoding)

    return df
