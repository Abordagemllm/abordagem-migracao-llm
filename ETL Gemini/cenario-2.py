import pandas as pd
import sqlalchemy

# Conexão com a base de dados de origem
host_origem = ''
port_origem = 15445
dbname_origem = ''
user_origem = ''
password_origem = ''

# Conexão com a base de dados de destino
host_destino = '127.0.0.1'
port_destino = 5432
dbname_destino = 'postgres'
user_destino = 'postgres'
password_destino = ''

# Query para selecionar os dados da base de origem
query = """
SELECT
  "e"."idempresa" AS "nk_empresa",
  "e"."razaosocial" AS "ds_razao_social",
  "c"."descrcidade" AS "ds_nome_cidade",
  "c"."uf" AS "ds_uf"
FROM "empresa" AS "e"
JOIN "cidade" AS "c"
  ON "e"."idcidade" = "c"."idcidade";
"""

# Estabelecendo a conexão com a base de origem
conn_origem = sqlalchemy.create_engine(
    f'postgresql://{user_origem}:{password_origem}@{host_origem}:{port_origem}/{dbname_origem}'
)

# Lendo os dados da base de origem
df = pd.read_sql(query, conn_origem)

# Estabelecendo a conexão com a base de destino
conn_destino = sqlalchemy.create_engine(
    f'postgresql://{user_destino}:{password_destino}@{host_destino}:{port_destino}/{dbname_destino}'
)

# Deletando os dados existentes na tabela de destino
df.to_sql('dim_empresa', conn_destino, schema='dw_ia', if_exists='replace', index=False)

# Fechando as conexões
conn_origem.dispose()
conn_destino.dispose()