from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

def enviar_email(sucesso):
    if(sucesso == 1):
        mensagem = 'A automação de atualização do Allnexus x Alldata rodou com sucesso.'
        assunto = 'SUCESSO - Automação de atualização do Allnexus x Alldata'
    else:
        mensagem = 'A automação de atualização do Allnexus x Alldata rodou falhou, por favor verifique o código no servidor.'
        assunto = 'FALHA - Automação de atualização do Allnexus x Alldata'
    # Configurações do servidor SMTP
    MAIL_HOST = "mail.allrede.net.br"
    MAIL_PORT = 465
    MAIL_USERNAME = "novosprodutosdev@allrede.net.br"
    MAIL_PASSWORD = "Devallrede.1010@"

    # Configurações do e-mail
    MAIL_FROM_ADDRESS = "novosprodutosdev@allrede.net.br"
    MAIL_FROM_NAME = "Allrede"
    MAIL_TO_ADDRESS = "leidiane.rodrigues@allrede.com.br,jorge.pacheco@allrede.com.br,samuel.silva@allrede.com.br"
    # "leidiane.rodrigues@allrede.com.br,amanda.lima@allrede.com.br"
    #MAIL_TO_ADDRESS = "jorge.pacheco@allrede.com.br,leidiane.rodrigues@allrede.com.br,amanda.lima@allrede.com.br,erick.oliveira@allrede.com.br"
    MAIL_SUBJECT = assunto
    MAIL_BODY = mensagem

    # Criar a mensagem de e-mail
    mensagem = MIMEMultipart()
    mensagem["From"] = f"{MAIL_FROM_NAME} <{MAIL_FROM_ADDRESS}>"
    # Use split para obter uma lista de destinatários
    to_addresses = MAIL_TO_ADDRESS.split(',')
    mensagem["To"] = ', '.join(to_addresses)
    mensagem["Subject"] = MAIL_SUBJECT

    # Adicionar corpo ao e-mail
    mensagem.attach(MIMEText(MAIL_BODY, "plain"))

    # Configurar conexão SMTP
    try:
        servidor_smtp = smtplib.SMTP_SSL(MAIL_HOST, MAIL_PORT)
        servidor_smtp.login(MAIL_USERNAME, MAIL_PASSWORD)

        # Enviar e-mail
        servidor_smtp.sendmail(MAIL_FROM_ADDRESS, to_addresses, mensagem.as_string())

        print("E-mail enviado com sucesso!")

    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")

    finally:
        # Fechar a conexão com o servidor SMTP
        if servidor_smtp:
            servidor_smtp.quit()