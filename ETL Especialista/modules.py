from sqlalchemy import create_engine
from urllib.parse import quote
import psycopg2
import os
import pandas as pd

def pg_cursor(pg_host, pg_port, pg_user, pg_pwd, pg_db):
    conn = psycopg2.connect(host=pg_host, port=pg_port, dbname=pg_db, user=pg_user, password=pg_pwd)
    cursor = conn.cursor()

    return cursor


def pg_connect(pg_host, pg_port, pg_user, pg_pwd, pg_db):
    postgres_str = ('postgresql+psycopg2://{username}:{password}@{ipaddress}:{port}/{dbname}'
                    .format(ipaddress=pg_host,
                            port=pg_port,
                            username=pg_user,
                            password=quote(pg_pwd),
                            dbname=pg_db)
                    )

    conn = create_engine(postgres_str)

    return conn


def get_sql_query(v_sql):
    script_dir = os.path.dirname('')  # Diretório do script atual
    sql_path = os.path.join(script_dir, 'sql', v_sql)  # Caminho absoluto para o arquivo SQL
    sql = open(sql_path, encoding='UTF-8').read()
    return sql


def compare_data_frames(df1, df2):
    merged_df = pd.merge(df1, df2, indicator=True, how='outer', suffixes=('_dw', '_db'))
    diff_rows = merged_df[merged_df['_merge'] == 'right_only']
    diff_rows = diff_rows.drop('_merge', axis=1)
    return diff_rows