import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Dados de conexão com a base de origem
orig_host = ""
orig_port = ""
orig_dbname = ""
orig_user = ""
orig_password = ""

# Dados de conexão com a base de destino
dest_host = "127.0.0.1"
dest_port = "5432"
dest_dbname = "postgres"
dest_user = "postgres"
dest_password = ""

# Query SQL
sql_query = """
SELECT 
    "n"."idplanilha" AS "nk_nota_fiscal",
    "n"."numnota" AS "nr_nota_fiscal",
    "n"."serienota" AS "ds_serie_nota_fiscal"
FROM 
    "nota" AS "n"
WHERE 
    "n"."dtmovimento" >= '2023-01-01' AND "n"."dtmovimento" <= '2023-12-31';
"""

# Conectar à base de dados de origem e extrair os dados
conn_orig = psycopg2.connect(
    dbname=orig_dbname, user=orig_user, password=orig_password, host=orig_host, port=orig_port
)
df = pd.read_sql(sql_query, conn_orig)
conn_orig.close()

# Conectar à base de dados de destino
conn_dest = psycopg2.connect(
    dbname=dest_dbname, user=dest_user, password=dest_password, host=dest_host, port=dest_port
)
cursor = conn_dest.cursor()

# Limpar a tabela de destino
cursor.execute("DELETE FROM dw_ia.dim_nota_fiscal")
conn_dest.commit()

# Usar o SQLAlchemy para criar um engine para inserção de dados
engine = create_engine(f'postgresql://{dest_user}:{dest_password}@{dest_host}:{dest_port}/{dest_dbname}')
df.to_sql('dim_nota_fiscal', engine, schema='dw_ia', if_exists='append', index=False)

# Fechar as conexões
conn_dest.close()
engine.dispose()

print("Dados transferidos com sucesso.")
