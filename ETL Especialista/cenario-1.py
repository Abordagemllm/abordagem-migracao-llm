import pandas as pd
import sys
import modules as modules


def dim_nota_fiscal():
    cursor_destino = modules.pg_cursor('127.0.0.1', '5432', 'postgres', '', 'postgres')
    conn_origem = modules.pg_connect('', '', '', '', '')
    conn_destino = modules.pg_connect('127.0.0.1', '5432', 'postgres', '', 'postgres')

    df_nota_fiscal_db = pd.read_sql_query(modules.get_sql_query('sql_nota_fiscal_db.sql'), conn_origem)

    cursor_destino.execute("TRUNCATE TABLE DW_MANUAL.DIM_NOTA_FISCAL;commit;")
    df_nota_fiscal_db.to_sql(schema='dw_manual', name='dim_nota_fiscal', con=conn_destino, if_exists='append', index=False, chunksize=100000)
