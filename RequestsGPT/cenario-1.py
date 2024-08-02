from langchain.agents import create_sql_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain_experimental.sql import SQLDatabaseChain
from langchain.sql_database import SQLDatabase
from dotenv import load_dotenv
import os
load_dotenv()
os.environ["OPENAI_API_KEY"] = ""

import psycopg2

db_host = ''
db_port = ''
db_name = ''
db_user = ''
db_password = ''
dialect = "postgresql"


from langchain.sql_database import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain.agents.agent_toolkits import SQLDatabaseToolkit

db = SQLDatabase.from_uri(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

from langchain.chat_models import ChatOpenAI
llm = ChatOpenAI(model_name="gpt-4-turbo")

toolkit = SQLDatabaseToolkit(db=db,llm=llm)

agent_executor = create_sql_agent(
    llm=llm,
    toolkit=toolkit,
    verbose=True
)

#db_chain = SQLDatabaseChain.from_llm(llm, db, verbose=True)
#response = db_chain.run("Retorne o texto do SQLQuery para uma consulta que busque os dados de nome do produto, relacionando com o preco e a empresa desse preco")

from langchain.chains import create_sql_query_chain

chain = create_sql_query_chain(llm, db)


solicitacao = f'''
Retorne o texto do SQLQuery para uma consulta que busque as colunas com os dados:
planilha da nota fiscal renomeando para nk_nota_fiscal
numero da nota fiscal renomeando pora nr_nota_fiscal
serie da nota fiscal renomeando para ds_serie_nota_fiscal

Busque apenas dados do período de 2023
'''


request = f'''You are a PostgreSQL expert. Given an input question, first create a syntactically correct SQLite query to run, then look at the results of the query and return the answer to the input question.
Unless the user specifies in the question a specific number of examples to obtain, do not limit the query in the first 5 rows retrieved using LIMIT. You can order the results to return the most informative data in the database.
Never query for all columns from a table. You must query only the columns that are needed to answer the question. Wrap each column name in double quotes (") to denote them as delimited identifiers.
Pay attention to use only the column names you can see in the tables below. Be careful to not query for columns that do not exist. Also, pay attention to which column is in which table.
Pay attention to use date('now') function to get the current date, if the question involves "today".

Use the following format:

SQLQuery: SQL Query to run

Always use initcap to varchar columns.

Question: {solicitacao}'''

sql = chain.invoke({"question": request})
print(sql)

host_destino = '127.0.0.1'
port_destino = '5432'
db_destino = 'postgres'
senha_destino = ''
usuario_destino = 'postgres'

chat = ChatOpenAI(model_name="gpt-4-turbo")

nome_tabela = 'dim_nota_fiscal'

input_ia = f'''
Com base nesse código: {sql}. Gere uma pipeline de dados em python utilizando pandas que insira estes dados em uma base de destino com os seguintes dados de acesso: 
host: {host_destino}, port: {port_destino}, dbname: {db_destino}, user: {usuario_destino} senha: {senha_destino}
Os dados da base de origem são os seguintes: host: {db_host}, port: {db_port}, dbname: {db_name}, user: {db_user} senha: {db_password}
A tabela de destino é em que os dados do SQL devem ser inseridos é a {nome_tabela} no shcema dw_ia.
Antes de inserir os dados na tabela, é necessário deletar os dados já existentes nela utilizando um cursor  do psycopg2 para deletar os dados.
'''

from langchain_core.messages import HumanMessage
chat.invoke(
    [
        HumanMessage(
            content=input_ia
        )
    ]
).pretty_print()