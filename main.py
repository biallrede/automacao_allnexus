import pandas as pd
from query import *
import schedule
from apscheduler.schedulers.background import BackgroundScheduler
import threading
from enviar_email import enviar_email
from fastapi import FastAPI
import time 

global status

app = FastAPI()

def verifica_atualizacao(tipo_execucao):
        status = 0
        try:
            if tipo_execucao == 2:
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
                print('rodou com sucesso a rotina')
            
            if tipo_execucao == 1:
                status = 1
                print('rotina ativa')
        except:
            print("Erro ao executar a rotina")

        return status

@app.get("/verifica_status")
def verifica_status_ativo():
    tipo_execucao = 1
    resposta = verifica_atualizacao(tipo_execucao)
    return resposta

def iniciar_agendador():
    tipo_execucao = 2
    scheduler = BackgroundScheduler()
    scheduler.add_job(verifica_atualizacao, "cron", hour=4, minute=00, args=[tipo_execucao])  # Agenda a rotina para 17:04 todos os dias
    scheduler.start()

    # Loop necessário para o `schedule` (opcional)
    while True:
        schedule.run_pending()
        time.sleep(1)

# Iniciar o agendador em uma thread separada
thread_agendador = threading.Thread(target=iniciar_agendador, daemon=True)
thread_agendador.start()

# scheduler = BackgroundScheduler()

# def rotina1():
#     verifica_atualizacao()

# schedule.every().day.at("03:00").do(rotina1)

# scheduler.start()

# while True:
#     schedule.run_pending()
#     threading.Event().wait(1)

# verifica_atualizacao()