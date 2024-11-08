from credentials import credenciais_banco_alldata, credenciais_banco_allnexus
import pandas as pd

def consulta_pessoas_alldata():
    conn = credenciais_banco_alldata()
    query = '''
                select 
                a.id_quadro,
                b.nome_razaosocial as name,
                b.cpf_cnpj,
                c.descricao as setor,
                d.nome as cargo,
                case when a.dt_demissao is not null then 0 else 1 end as status
				--case when a.data_ult_alteracao is null then '1900-01-01' else a.data_ult_alteracao end  as data_ult_alteracao
                from QUADRO_FT a
                left join DOMINIO_DADOS_PESSOAIS b on b.id_dados_pessoais = a.id_dados_pessoais
                left join HIERARQUIA_SETOR c on c.id_setor = a.id_setor
                left join DOMINIO_DM_CARGO d on d.id_cargo = a.id_cargo
                where a.dt_demissao is null
                '''
    
    df = pd.read_sql(query,conn)
    
    return df

def consulta_pessoas_allnexus():
    conn = credenciais_banco_allnexus()
    query = '''
                select name, 
                cpf_cnpj, 
                setor, cargo, id_quadro
                from users                    
                '''
    
    df = pd.read_sql(query,conn)

    return df

def atualiza_cadastro_banco_allnexus(nome_razaosocial,id_quadro,setor,cargo,ativo,cpf_cnpj):
    conn = credenciais_banco_allnexus()
    query = '''
                UPDATE public.users
                SET 
                id_quadro={id_quadro},
                cpf_cnpj='{cpf_cnpj}',
                name='{nome_razaosocial}', 
                setor='{setor}', 
                cargo='{cargo}', 
                ativo={ativo},  
                updated_at= NOW()
                WHERE name like '%{nome_razaosocial}%';                   
                '''.format(nome_razaosocial=nome_razaosocial,id_quadro=id_quadro,setor=setor,cargo=cargo,ativo=ativo,cpf_cnpj=cpf_cnpj)
    # print(query)
    try:
        # Abrindo um cursor para executar a query de atualização
        with conn.cursor() as cursor:
            cursor.execute(query)  # Executa a query de atualização
            conn.commit()  # Confirma a transação
            print("Atualização realizada com sucesso.")
    except Exception as e:
        print(f"Erro ao executar a atualização: {e}")

