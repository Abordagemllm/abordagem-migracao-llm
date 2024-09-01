import psycopg2
from psycopg2 import Error

# Configurações da base de destino (dw_ia)
host_destino = ''
porta_destino = 0
dbname_destino = ''
user_destino = ''
senha_destino = ''

# Configurações da base de origem (postgres)
host_origem = ''
porta_origem = 0
dbname_origem = ''
user_origem = ''
senha_origem = ''

# Tabela de destino em dw_ia
tabela_destino = 'dw_ia.fat_produto_movimento'

try:
    # Conexão com a base de destino (dw_ia)
    conn_destino = psycopg2.connect(
        host=host_destino,
        port=porta_destino,
        database=dbname_destino,
        user=user_destino,
        password=senha_destino
    )

    # Conexão com a base de origem (postgres)
    conn_origem = psycopg2.connect(
        host=host_origem,
        port=porta_origem,
        database=dbname_origem,
        user=user_origem,
        password=senha_origem
    )

    # Cria um cursor para executar queries na base de destino
    cur_destino = conn_destino.cursor()

    # Deleta os dados existentes na tabela de destino
    query_deletar = "TRUNCATE TABLE dw_ia.fat_produto_movimento"
    cur_destino.execute(query_deletar)
    conn_destino.commit()

    # Cria um cursor para executar queries na base de origem
    cur_origem = conn_origem.cursor()

    # Executa a query do PostgreSQL para obter os dados
    query_postgresql = """
        SELECT 
            '"' || CONCAT('planilha do movimento', '-', 'numsequencia') || '"' AS nk_produto_movimento,
            idproduto AS nk_produto,
            idempresa AS nk_empresa,
            idlocalestoque AS nk_local_estoque,
            idplanilha AS nk_nota_fiscal,
            idoperacao AS nk_operacao,
            0 AS nk_pessoa,
            valtotliquido AS vl_total_liquido,
            qtdproduto AS vl_qtd_produto,
            dtmovimento AS dt_movimento
        FROM 
            estoque_analitico
        WHERE 
            dtmovimento >= CURRENT_DATE - INTERVAL '2 month'
        ORDER BY 
            dtmovimento DESC;
    """
    cur_origem.execute(query_postgresql)

    # Obtem os resultados da query do PostgreSQL
    resultado = cur_origem.fetchall()

    # Insere os dados na tabela de destino
    query_inserir = """
        INSERT INTO dw_ia.fat_produto_movimento (
            nk_produto_movimento,
            nk_produto,
            nk_empresa,
            nk_local_estoque,
            nk_nota_fiscal,
            nk_operacao,
            nk_pessoa,
            vl_total_liquido,
            vl_qtd_produto,
            dt_movimento
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cur_destino.executemany(query_inserir, resultado)
    conn_destino.commit()

    # Fechamento das conexões
    cur_origem.close()
    cur_destino.close()
    conn_origem.close()
    conn_destino.close()

except Error as e:
    print(f"Erro: {e}")
