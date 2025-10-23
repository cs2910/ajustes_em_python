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

query = text("select protocolo from cnv_id_atendom where protocolo is not null "
             "and id_gerado is null and sequenc > 538541 "
             "group by protocolo")
##query = text("select * from ecocep where novocep is not null and codigo = '1'")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
for tabela in resultados:
    print(f'{tabela.protocolo}')
    f_token_retorno = tabela.protocolo
    status_lote = 'SEMREGISTRO'
    while status_lote != "EXECUTADO" and status_lote != "EXECUTADO_PARCIALMENTE":
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
                if status_lote == "EXECUTADO" or  status_lote == "EXECUTADO_PARCIALMENTE":
                    for item in retornos:
                        id_integracao = item.get('idIntegracao')
                        id_gerado = item.get('idGerado')
                        mensaErro = item.get('mensagem')
                        print(mensaErro)
                        status = item.get('status')
                        print(f"ID Integração: {id_integracao}, ID Gerado: {id_gerado}, Status: {status}")
                        if status == "SUCESSO":
                            stmt = text(
                                f"UPDATE cnv_id_atendom SET id_gerado = {id_gerado}, mensagemerro = null WHERE  sequenc = {id_integracao}")
                            session.execute(stmt)
                            session.commit()
                        if status == "ERRO":
                            stmt = text(
                                f"UPDATE cnv_id_atendom SET mensagemerro = '{mensaErro}' WHERE  sequenc = {id_integracao}")
                            session.execute(stmt)
                            session.commit()
            except json.JSONDecodeError:
                print("Erro: A resposta não é um JSON válido.")
            except KeyError as e:
                print(f"Erro: Chave ausente no JSON - {e}")
        else:
            print(f'erro execução')
    query = text(f"select id_gerado from cnv_id_atendom where protocolo = '{tabela.protocolo}'")
    resultadoId = session.execute(query).fetchall()
    for tabelaId in resultadoId:
        f_token_id = tabelaId.id_gerado
        print(f_token_id)
        if f_token_id is not None:
            response = requests.get(
                f'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentosDomiciliares/{f_token_id}',
                headers={'Authorization': f'Bearer {tokenEntidade}'})
            status = response.status_code
            print(response.content)
            print(status)
            if status == 200:
                recurrenceId = json.loads(response.content)
                f_token_codigo = recurrenceId['codigo']
                print(f'protocolo {f_token_codigo}')
                stmt = text(
                    f"UPDATE cnv_id_atendom SET codigo = {f_token_codigo} WHERE  id_gerado = {tabelaId.id_gerado}")
                session.execute(stmt)
                session.commit()