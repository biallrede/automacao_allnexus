import pandas as pd
from query import *
import schedule
from apscheduler.schedulers.background import BackgroundScheduler
import threading
from datetime import datetime, date
import time

# Função para ler o valor do arquivo
def ler_valor_arquivo(nome_arquivo):
    try:
        with open(nome_arquivo, 'r') as arquivo:
            valor = arquivo.read().strip()  # Lê e remove espaços em branco
            return int(valor)  # Converte para inteiro
    except FileNotFoundError:
        print("Arquivo não encontrado. Criando um novo arquivo.")
        return None
    except ValueError:
        print("Erro: O conteúdo do arquivo não é um número válido.")
        return None

# Função para atualizar o valor do arquivo
def atualizar_valor_arquivo(nome_arquivo, novo_valor):
    with open(nome_arquivo, 'w') as arquivo:
        arquivo.write(str(novo_valor))  # Escreve o novo valor no arquivo
    print(f"Valor atualizado no arquivo: {novo_valor}")

# Comparar o valor da variável com o valor do arquivo
def comparar_e_atualizar(nome_arquivo, valor_variavel):
    valor_arquivo = ler_valor_arquivo(nome_arquivo)

    if valor_arquivo is None or valor_variavel > valor_arquivo:
        print(f"O valor da variável ({valor_variavel}) é maior do que o valor do arquivo ({valor_arquivo}).")
        atualizar_valor_arquivo(nome_arquivo, valor_variavel)
    else:
        print(f"O valor da variável ({valor_variavel}) não é maior do que o valor do arquivo ({valor_arquivo}).")

def verifica_atualizacao():
    df_demitidos = consulta_qtd_demitidos()
    contador_demitidos = df_demitidos.loc[0,'qtd_demitidos']
    valor_arquivo = ler_valor_arquivo('qtd_demitidos.txt')
    # comparar_e_atualizar('qtd_demitidos.txt', contador_demitidos)
    if int(contador_demitidos) > int(valor_arquivo):
        df_alldata = consulta_pessoas_alldata()
        df_alldata = df_alldata.loc[df_alldata.groupby('cpf_cnpj')['data_ult_alteracao'].idxmax()]
        df_allnexus = consulta_pessoas_allnexus() 
        # df_allnexus = df_allnexus.drop(columns=['ativo'])
        # df_alldata = df_alldata.drop(columns=['status','data_ult_alteracao'])

        df_merge = df_allnexus.merge(df_alldata,on='cpf_cnpj',how='inner', suffixes=('_allnexus', '_alldata'))
        print(df_merge)
        for index, row in df_merge.iterrows():

            if df_merge.loc[index,'ativo'] != df_merge.loc[index,'status']:
                df_merge.loc[index,'ativo'] = df_merge.loc[index,'status']

            if df_merge.loc[index,'setor_allnexus'] != df_merge.loc[index,'setor_alldata']:
                df_merge.loc[index,'setor_allnexus'] = df_merge.loc[index,'setor_alldata']
            
            if df_merge.loc[index,'cargo_allnexus'] != df_merge.loc[index,'cargo_alldata']:
                df_merge.loc[index,'cargo_allnexus'] = df_merge.loc[index,'cargo_alldata']

        df_merge = df_merge.drop(['status', 'setor_alldata', 'cargo_alldata', 'data_ult_alteracao','nome_razaosocial_alldata'], axis=1)
        df_merge = df_merge.rename(columns={'setor_allnexus': 'setor', 'cargo_allnexus': 'cargo', 'nome_razaosocial_allnexus':'nome_razaosocial'})

        try:
            for index, row in df_merge.iterrows:
                nome_razaosocial = str(df_merge.loc[index,'cargo'])
                cpf_cnpj = str(df_merge.loc[index,'cpf_cnpj'])
                setor = str(df_merge.loc[index,'setor'])
                cargo = str(df_merge.loc[index,'cargo'])
                if df_merge.loc[index,'ativo'] == 1:
                    ativo = bool('true')
                else:
                    ativo = bool('false')
                atualiza_cadastro_banco_allnexus(nome_razaosocial,cpf_cnpj,setor,cargo,ativo)
            print('Dados atualizados com sucesso.')
        except Exception as e:
            print(f"Falha na tentativa: {e}".format(e))



        


# def inserir_banco_allnexus(df_atualizado):


# def inicializa(data_ultima_consulta):
    

# data_ultima_consulta = ultima_verificacao = time.strftime('%Y-%m-%d %H:%M:%S')

# scheduler = BackgroundScheduler()

# def rotina1():
#     inicializa(data_ultima_consulta)

# schedule.every().day.at("03:00").do(rotina1)

# scheduler.start()

# while True:
#     schedule.run_pending()
#     threading.Event().wait(1)

verifica_atualizacao()