import pandas as pd
from query import *
import schedule
from apscheduler.schedulers.background import BackgroundScheduler
import threading
from enviar_email import enviar_email


def verifica_atualizacao():
        df_alldata = consulta_pessoas_alldata()
        df_allnexus = consulta_pessoas_allnexus() 
        df_merge = df_allnexus.merge(df_alldata,on='name',how='inner', suffixes=('_allnexus', '_alldata')).reindex()
        # print(df_merge.columns)
        # df_merge.to_excel('teste.xlsx',index=False)
        try:
            for index, row in df_merge.iterrows():
                cpf_cnpj = str(df_merge.loc[index,'cpf_cnpj_alldata'])
                nome_razaosocial = str(df_merge.loc[index,'name'])
                # print(nome_razaosocial)
                id_quadro = int(df_merge.loc[index,'id_quadro_alldata'])
                # print(id_quadro)
                setor = str(df_merge.loc[index,'setor_alldata'])
                cargo = str(df_merge.loc[index,'cargo_alldata'])
                valor = df_merge.loc[index,'status'] 
                string = 'false'
                if valor == 1:
                    ativo = bool('true')
                if valor == 0:
                    ativo = (string.lower() != 'false')
                atualiza_cadastro_banco_allnexus(nome_razaosocial,id_quadro,setor,cargo,ativo,cpf_cnpj)
            print('Dados atualizados com sucesso.')
            sucesso = 1
        except Exception as e:
            print(f"Falha na tentativa: {e}".format(e))
            sucesso = 0
        
        enviar_email(sucesso)



scheduler = BackgroundScheduler()

def rotina1():
    verifica_atualizacao()

schedule.every().day.at("03:00").do(rotina1)

scheduler.start()

while True:
    schedule.run_pending()
    threading.Event().wait(1)

# verifica_atualizacao()