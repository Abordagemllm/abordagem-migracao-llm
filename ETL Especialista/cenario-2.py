import pandas as pd
import sys
import modules as modules


def dim_empresa():
    cursor_destino = modules.pg_cursor('127.0.0.1', '5432', 'postgres', '', 'postgres')
    conn_origem = modules.pg_connect('', '', '', '', '')
    conn_destino = modules.pg_connect('127.0.0.1', '5432', 'postgres', '', 'postgres')

    df_produtos_db = pd.read_sql_query(modules.get_sql_query('sql_empresas_db.sql'), conn_origem)

    cursor_destino.execute("TRUNCATE TABLE DW_MANUAL.DIM_EMPRESA;commit;")
    df_produtos_db.to_sql(schema='dw_manual', name='dim_empresa', con=conn_destino, if_exists='append', index=False, chunksize=100000)
