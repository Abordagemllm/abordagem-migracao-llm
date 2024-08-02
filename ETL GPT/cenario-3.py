import pandas as pd
import psycopg2
from sqlalchemy import create_engine


# Definição das credenciais de conexão do banco de dados de origem
origem_db_config = {
    "host": "",
    "port": "",
    "dbname": "",
    "user": "",
    "password": ""
}

# Definição das credenciais de conexão do banco de dados de destino
destino_db_config = {
    "host": "127.0.0.1",
    "port": "5432",
    "dbname": "postgres",
    "user": "postgres",
    "password": ""
}

# SQL para extrair dados
sql_query = """
SELECT 
    concat(ea.idplanilha, '_', ea.numsequencia) AS nk_produto_movimento,
    ea.idproduto AS nk_produto,
    ea.idempresa AS nk_empresa,
    ea.idlocalestoque AS nk_local_estoque,
    n.idplanilha AS nk_nota_fiscal,
    ea.idoperacao AS nk_operacao,
    fp.idpessoa AS nk_pessoa,
    ea.valtotliquido AS vl_total_liquido,
    ea.qtdproduto AS vl_qtd_produto,
    ea.dtmovimento AS dt_movimento
FROM 
    estoque_analitico AS ea
JOIN 
    nota AS n ON ea.idplanilha = n.idplanilha AND ea.idempresa = n.idempresa
JOIN 
    forma_pagamento AS fp ON n.idforma = fp.idforma
WHERE 
    ea.dtmovimento >= CURRENT_DATE - INTERVAL '2 months';
"""

# Conectar com o banco de dados de origem e extrair dados
conn_origem = psycopg2.connect(**origem_db_config)
df = pd.read_sql_query(sql_query, conn_origem)
conn_origem.close()

# Conectar com o banco de dados de destino
conn_destino = psycopg2.connect(**destino_db_config)
cursor = conn_destino.cursor()

# Limpar dados existentes na tabela dim_produto
cursor.execute("DELETE FROM dw_ia.fat_produto_movimento;")
conn_destino.commit()

# Inserir dados na tabela dim_produto usando pandas e sqlalchemy
engine = create_engine(f"postgresql://{destino_db_config['user']}:{destino_db_config['password']}@{destino_db_config['host']}:{destino_db_config['port']}/{destino_db_config['dbname']}")
df.to_sql('dim_produto', engine, schema='dw_ia', if_exists='append', index=False)

# Fechar conexões
cursor.close()
conn_destino.close()
engine.dispose()

print("Dados transferidos com sucesso!")
