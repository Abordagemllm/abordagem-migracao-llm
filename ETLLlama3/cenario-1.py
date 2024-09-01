import pandas as pd
import psycopg2

def etl_nota_fiscal():
    # Configurações da base de origem
    host_origem = ''
    port_origem = 0
    dbname_origem = ''
    user_origem = ''
    senha_origem = ''

    # Configurações da base de destino
    host_destino = ''
    port_destino = 0
    dbname_destino = ''
    user_destino = ''
    senha_destino = ''

    # Conecta à base de origem
    conn_origem = psycopg2.connect(
        host=host_origem,
        port=port_origem,
        database=dbname_origem,
        user=user_origem,
        password=senha_origem
    )

    # Conecta à base de destino
    conn_destino = psycopg2.connect(
        host=host_destino,
        port=port_destino,
        database=dbname_destino,
        user=user_destino,
        password=senha_destino
    )

    # Executa a query para buscar os dados da nota fiscal
    cur_origem = conn_origem.cursor()
    query = """
        SELECT 
            idplanilha AS "nk_nota_fiscal",
            numnota AS "nr_nota_fiscal",
            serienota AS "ds_serie_nota_fiscal"
        FROM 
            nota
        WHERE 
            EXTRACT(YEAR FROM dtmovimento) = 2023;
    """
    cur_origem.execute(query)
    dados_notas_fiscais = cur_origem.fetchall()

    # Deleta os dados já existentes na tabela de destino
    cur_destino = conn_destino.cursor()
    query_deletar = "TRUNCATE TABLE dw_ia.dim_nota_fiscal;"
    cur_destino.execute(query_deletar)

    # Insere os dados na tabela de destino
    df_notas_fiscais = pd.DataFrame(dados_notas_fiscais, columns=['nk_nota_fiscal', 'nr_nota_fiscal', 'ds_serie_nota_fiscal'])
    cur_destino.executemany("INSERT INTO dw_ia.dim_nota_fiscal (nk_nota_fiscal, nr_nota_fiscal, ds_serie_nota_fiscal) VALUES (%s, %s, %s);", df_notas_fiscais.values)

    # Comita as alterações
    conn_destino.commit()

    # Fecha as conexões
    cur_origem.close()
    cur_destino.close()
    conn_origem.close()
    conn_destino.close()

%timeit etl_nota_fiscal()