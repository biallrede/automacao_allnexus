import pandas as pd
from query import *
import schedule
from apscheduler.schedulers.background import BackgroundScheduler
import threading


def verifica_atualizacao():
        df_alldata = consulta_pessoas_alldata()
        df_alldata = df_alldata.loc[df_alldata.groupby('cpf_cnpj')['data_ult_alteracao'].idxmax()]
        df_allnexus = consulta_pessoas_allnexus() 

        df_merge = df_allnexus.merge(df_alldata,on='cpf_cnpj',how='inner', suffixes=('_allnexus', '_alldata'))
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
            for index, row in df_merge.iterrows():
                nome_razaosocial = str(df_merge.loc[index,'nome_razaosocial'])
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



scheduler = BackgroundScheduler()

def rotina1():
    verifica_atualizacao()

schedule.every().day.at("03:00").do(rotina1)

scheduler.start()

while True:
    schedule.run_pending()
    threading.Event().wait(1)

# verifica_atualizacao()