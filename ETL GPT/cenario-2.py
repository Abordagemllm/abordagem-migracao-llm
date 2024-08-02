import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Informações do banco de dados de origem
origem_db_config = {
    "host": "",
    "port": "",
    "dbname": "",
    "user": "",
    "password": ""
}

# Informações do banco de dados de destino
destino_db_config = {
    "host": "127.0.0.1",
    "port": "5432",
    "dbname": "postgres",
    "user": "postgres",
    "password": ""
}

# Conexão com o banco de dados de origem
conn_origem = psycopg2.connect(**origem_db_config)

# Query para extração de dados
query = """
SELECT empresa.idempresa AS nk_empresa,
       initcap(empresa.razaosocial) AS ds_razao_social,
       initcap(cidade.descrcidade) AS ds_nome_cidade,
       cidade.uf AS ds_uf
FROM empresa
JOIN cidade ON empresa.idcidade = cidade.idcidade;
"""

# Carrega os dados em um DataFrame
df = pd.read_sql(query, conn_origem)

# Fecha a conexão de origem
conn_origem.close()

# Conexão com o banco de dados de destino
conn_destino = psycopg2.connect(**destino_db_config)
cursor = conn_destino.cursor()

# Limpa os dados da tabela de destino antes de inserir novos dados
cursor.execute("DELETE FROM dw_ia.dim_empresa")
conn_destino.commit()

# Cria um engine para usar com pandas to_sql
engine_str = f"postgresql+psycopg2://{destino_db_config['user']}:{destino_db_config['password']}@{destino_db_config['host']}:{destino_db_config['port']}/{destino_db_config['dbname']}"
engine = create_engine(engine_str)

# Insere os dados no banco de destino
df.to_sql('dim_empresa', engine, schema='dw_ia', if_exists='append', index=False)

# Fecha a conexão de destino
conn_destino.close()

print("Dados transferidos com sucesso!")