import psycopg2
from sqlalchemy import create_engine

def credenciais_banco_alldata():
# Configuração da conexão com o banco de dados
    server = '187.121.151.19'
    database = 'DB_ALLNEXUS'
    username = 'user_allnexus'
    password = 'uKl041xn8HIw0WF'

    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
    engine = create_engine(connection_string,fast_executemany=True)
    return engine

def credenciais_banco_allnexus():
    conn = psycopg2.connect(
                        host='177.66.167.90',
                        port='5432',
                        database='allnexus',
                        user='novosprodutos',
                        password='NovosProdutos@2024')
    
    return conn