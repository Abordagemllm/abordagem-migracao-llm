import psycopg2
from psycopg2 import Error
import pandas as pd

# Configurações do banco de dados de origem
host = ''
port = 0
dbname = ''
user = ''
senha = ''

try:
    # Abre uma conexão com o banco de dados
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=dbname,
        user=user,
        password=senha
    )
except Error as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
    conn = None

if conn:
    # Etapa 2: Executar a consulta SQL para selecionar os dados
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            "nk_empresa" AS nk_empresa, 
            initcap("razaosocial") AS ds_razao_social,
            initcap("descrcidade") AS ds_nome_cidade,
            initcap("uf") AS ds_uf
        FROM empresa;
    """)

    # Obtém os dados da consulta SQL e armazena em um DataFrame pandas
    df = pd.DataFrame(cur.fetchall(), columns=['nk_empresa', 'ds_razao_social', 'ds_nome_cidade', 'ds_uf'])
    cur.close()

    # Etapa 3: Conectar ao banco de dados de destino
    host_destino = ''
    port_destino = 0
    dbname_destino = ''
    user_destino = ''
    senha_destino = ''

    try:
        # Abre uma conexão com o banco de dados de destino
        conn_destino = psycopg2.connect(
            host=host_destino,
            port=port_destino,
            database=dbname_destino,
            user=user_destino,
            password=senha_destino
        )
    except Error as e:
        print(f"Erro ao conectar ao banco de dados de destino: {e}")
        conn_destino = None

    if conn_destino:
        # Etapa 4: Executar a consulta SQL para deletar os dados existentes na tabela de destino
        cur_destino = conn_destino.cursor()
        cur_destino.execute("DELETE FROM dw_ia.dim_empresa")
        conn_destino.commit()
        cur_destino.close()

        # Etapa 5: Insere os dados no banco de dados de destino
        cur_destino = conn_destino.cursor()

        # Insere os dados do DataFrame pandas na tabela de destino
        for index, row in df.iterrows():
            cur_destino.execute("""
                INSERT INTO dw_ia.dim_empresa (nk_empresa, ds_razao_social, ds_nome_cidade, ds_uf)
                VALUES (%s, %s, %s, %s);
            """, (
                row['nk_empresa'],
                row['ds_razao_social'],
                row['ds_nome_cidade'],
                row['ds_uf']
            ))
        conn_destino.commit()
        cur_destino.close()

        # Fecha as conexões
        conn_destino.close()

    conn.close()
