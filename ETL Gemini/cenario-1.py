import pandas as pd
import psycopg2

# Conexão com a base de dados de origem
conn_origem = psycopg2.connect(
    host="",
    port="",
    dbname="",
    user="",
    password="",
)

# Consulta SQL para obter os dados da base de origem
sql = """
SELECT 
  "idplanilha" AS "nk_nota_fiscal", 
  "numnota" AS "nr_nota_fiscal", 
  "serienota" AS "ds_serie_nota_fiscal" 
FROM 
  "nota" 
WHERE 
  "dtmovimento" >= '2023-01-01' 
  AND "dtmovimento" <= '2023-12-31';
"""

# Lê os dados da base de origem para um DataFrame do Pandas
df = pd.read_sql(sql, conn_origem)

# Fecha a conexão com a base de origem
conn_origem.close()

# Conexão com a base de dados de destino
conn_destino = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="",
)

# Cria um cursor para deletar os dados existentes na tabela de destino
cursor = conn_destino.cursor()

# Deleta os dados existentes na tabela de destino
cursor.execute("DELETE FROM dw_ia.dim_nota_fiscal")

# Confirma a deleção
conn_destino.commit()

# Insere os dados do DataFrame do Pandas na tabela de destino
df.to_sql("dim_nota_fiscal", conn_destino, schema="dw_ia", if_exists="append")

# Fecha o cursor e a conexão com a base de dados de destino
cursor.close()
conn_destino.close()
