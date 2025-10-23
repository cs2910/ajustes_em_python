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

tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

'''Utilizando select direto'''

query = text("select protocolodelete from cnv_atendprofissional where id_delete is null"
             " and protocolodelete is not null group by  protocolodelete ")

resultados = session.execute(query).fetchall()
for tabela in resultados:
    print(f'{tabela.protocolodelete}')
    f_token_retorno = tabela.protocolodelete
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
                        id_gerado = item.get('idGerado')
                        mensaErro = item.get('mensagem')
                        print(mensaErro)
                        status = item.get('status')
                        print(f"ID Integração: {id_integracao}, ID Gerado: {id_gerado}, Status: {status}")
                        if status == "SUCESSO":
                            stmt = text(
                                f"UPDATE cnv_atendprofissional SET id_delete = {id_gerado} WHERE  sequenc = {id_integracao}")
                            ##print(stmt)
                            session.execute(stmt)
                            session.commit()
                        if status == "ERRO":
                            stmt = text(
                                f"UPDATE cnv_atendprofissional SET menssagemerrodelete = '{mensaErro}' WHERE  sequenc = {id_integracao}")
                            session.execute(stmt)
                            session.commit()
            except json.JSONDecodeError:
                print("Erro: A resposta não é um JSON válido.")
            except KeyError as e:
                print(f"Erro: Chave ausente no JSON - {e}")
        else:
            print(f'erro execução')