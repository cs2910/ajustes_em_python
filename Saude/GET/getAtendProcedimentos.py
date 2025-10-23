from sqlalchemy.orm import sessionmaker
from sqlalchemy import extract, update,insert, text
from tabelatributos import Pensend
from conexao import engine, connection
import requests, json

#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Linka
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/consultalote/'
#### Token da Endidade

tokenEntidade = 'fe8704a7-f0da-41f9-91d1-acf75a1d5a0f'

'''Utilizando select direto'''
## Atendimentos sem prontuarios
query = text("select protocoloprocante from cnv_atendprontuario where "
             "protocoloprocante is not null and id_proced is null "
             "and mensagemerroproc ilike '%CBO%' group by protocoloprocante")

resultados = session.execute(query).fetchall()
for tabela in resultados:
    print(f'{tabela.protocoloprocante}')
    f_token_retorno = tabela.protocoloprocante
    status_lote = 'SEMREGISTRO'
    while status_lote != "EXECUTADO" and status_lote != "EXECUTADO_PARCIALMENTE" and status_lote != "ERRO":
        response = requests.get(f'https://saude.betha.cloud/api-saude-service-layer/v2/api/consultalote/{f_token_retorno}',
                           headers={'Authorization': f'Bearer {tokenEntidade}'})
        status = response.status_code
        print(response.content)
        print(status)
        if status == 200:
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f'protocolo {f_token_retorno}')
            try:
               ## json_string = response_data.decode('utf-8')
                data = recurrence
                status_lote = data.get('statusLote')
                retornos = data.get('retorno', [])
                print(f"Status do Lote: {status_lote}")
                print("--- Detalhes do Retorno ---")
                if status_lote == "EXECUTADO" or  status_lote == "EXECUTADO_PARCIALMENTE" or status_lote == "ERRO":
                    for item in retornos:
                        id_integracao = item.get('idIntegracao')
                        id_procedimento = item.get('idProcedimento')
                        id_gerado = item.get('idGerado')
                        mensaErro = item.get('mensagem')
                        if mensaErro is None:
                            mensaErro = 'Null'
                        print(mensaErro)
                        status = item.get('status')
                        print(f"ID Integração: {id_integracao}, ID Gerado: {id_gerado}, Status: {status}")
                        if status == "SUCESSO":
                            stmt = text(f"update cnv_atendprontuario set id_proced = {id_gerado},mensagemerroproc = Null where sequenc = {id_integracao} and protocoloprocante = '{f_token_retorno}'")
                            session.execute(stmt)
                            session.commit()
                        if status == "ERRO":
                            stmt = text(f"update cnv_atendprontuario set mensagemerroproc = '{mensaErro}' where sequenc = {id_integracao} and protocoloprocante = '{f_token_retorno}'")
                            print(stmt)
                            session.execute(stmt)
                            session.commit()
            except json.JSONDecodeError:
                print("Erro: A resposta não é um JSON válido.")
            except KeyError as e:
                print(f"Erro: Chave ausente no JSON - {e}")
        else:
            print(f'erro execução')
