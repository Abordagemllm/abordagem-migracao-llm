import pandas as pd
import sys
import modules as modules


def compare_data_frames(df1, df2):
    merged_df = pd.merge(df1, df2, indicator=True, how='outer', suffixes=('_dw', '_db'))
    diff_rows = merged_df[merged_df['_merge'] == 'right_only']
    diff_rows = diff_rows.drop('_merge', axis=1)
    return diff_rows


def registro_excluido(df1, df2):
    merged_df = pd.merge(df1, df2, indicator=True, how='outer', suffixes=('_dw', '_db'))
    diff_rows = merged_df[merged_df['_merge'] == 'left_only']
    diff_rows = diff_rows.drop('_merge', axis=1)
    return diff_rows


def delete_registro(df1):
    cursor_destino = modules.pg_cursor('127.0.0.1', '5432', 'postgres', '', 'postgres')
    lista = df1['nk_produto_movimento'].tolist()
    lista_str = ','.join(str(num) for num in lista)
    delete_registros = f'DELETE FROM DW_MANUAL.FAT_PRODUTO_MOVIMENTO WHERE NK_PRODUTO_MOVIMENTO IN ({lista_str});COMMIT;'
    if len(lista) > 0:
        cursor_destino.execute(delete_registros)


def fat_produto_movimento():
    conn_origem = modules.pg_connect('', '', '', '', '')
    conn_destino = modules.pg_connect('127.0.0.1', '5432', 'postgres', '', 'postgres')

    df_produto_movimento_db = pd.read_sql_query(modules.get_sql_query('sql_produto_movimento_db.sql'), conn_origem)
    df_produto_movimento_dw = pd.read_sql_query(modules.get_sql_query('sql_produto_movimento_dw.sql'), conn_destino)

    df_produto_movimento_insert = compare_data_frames(df_produto_movimento_dw, df_produto_movimento_db)

    delete_registro(df_produto_movimento_insert)
    delete_registro(registro_excluido(df_produto_movimento_dw, df_produto_movimento_db))
    df_produto_movimento_insert.to_sql(schema='dw_manual', name='fat_produto_movimento', con=conn_destino, if_exists='append', index=False, chunksize=100000)
