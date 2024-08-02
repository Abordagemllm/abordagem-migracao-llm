import pandas as pd
import psycopg2

# Estabelece uma conexão com o banco de dados de origem
conn_origem = psycopg2.connect(
    host="",
    port=0,
    dbname="",
    user="",
    password="",
)

# Executa a consulta SQL no banco de dados de origem
query = """
SELECT "estoque_analitico"."idplanilha" || "estoque_analitico"."numsequencia" AS "nk_produto_movimento",
       "estoque_analitico"."idproduto" AS "nk_produto",
       "estoque_analitico"."idempresa" AS "nk_empresa",
       "estoque_analitico"."idlocalestoque" AS "nk_local_estoque",
       "nota"."idplanilha" AS "nk_nota_fiscal",
       "estoque_analitico"."idoperacao" AS "nk_operacao",
       "nota"."idclifor" AS "nk_pessoa",
       "estoque_analitico"."valtotliquido" AS "vl_total_liquido",
       "estoque_analitico"."qtdproduto" AS "vl_qtd_produto",
       "estoque_analitico"."dtmovimento" AS "dt_movimento"
FROM "estoque_analitico"
JOIN "nota" ON "estoque_analitico"."idempresa" = "nota"."idempresa"
WHERE "estoque_analitico"."dtmovimento" >= date('now', '-2 months')
"""
df = pd.read_sql_query(query, conn_origem)

# Fecha a conexão com o banco de dados de origem
conn_origem.close()

# Estabelece uma conexão com o banco de dados de destino
conn_destino = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="",
)

# Cria um cursor para deletar os dados existentes na tabela de destino
cursor = conn_destino.cursor()
cursor.execute("DELETE FROM dw_ia.dim_produto")
conn_destino.commit()

# Insere os dados no banco de dados de destino
df.to_sql("dim_produto", conn_destino, schema="dw_ia", if_exists="append", index=False)

# Fecha a conexão com o banco de dados de destino
conn_destino.close()
#%%
