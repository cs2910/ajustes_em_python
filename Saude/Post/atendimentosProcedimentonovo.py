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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoProcedimentoRealizados'
#### Token da Endidade

tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

'''Utilizando select direto'''

query = text("select a.sequenc,id_profissional,id_procedimento,id_especialidade,id_cbo,id_cidadao, a.id_gerado as idatendimentos from public.cnv_atendprontuario ap "
             "inner join cnv_atendimento a on "
             "a.sequenc = ap.sequenc where id_proced is null"
             " order by a.sequenc ")

resultados = session.execute(query).fetchall()
batch_size = 1
data = []
for tabela in resultados:
    print(f'{tabela.sequenc} -  {tabela.id_procedimento} - Id {tabela.idatendimentos}')
    proced = tabela.id_procedimento
    data.append({
        "idIntegracao": f"{tabela.sequenc}",
        "atendimentoProcedimentoRealizado": {
            "atendimento":{
                "id":  tabela.idatendimentos
            },
            "procedimento": {
                "id" : tabela.id_procedimento
            },
            "profissional":{
                "id": tabela.id_profissional
            },
            "especialidade":{
                "id": tabela.id_especialidade
            },
            "cbo":{
                "id": tabela.id_cbo
            },
            "cliente":{
                "id": tabela.id_cidadao
            },
            "quantidade": 1
        }
    } )

    print(len(data))
    if len(data) == batch_size:
        print(data)
    ##    dados = json.dumps(data, ensure_ascii=False)
        try:
            dados = json.dumps(data, ensure_ascii=False) ## , indent=4 mostra o json em arvore
            print("Conversão para JSON bem-sucedida!")
            print(dados)
        except TypeError as e:
            print(f"Ocorreu um erro: {e}")
        print(dados)

        response = requests.post(urlPost,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(data))
        status = response.status_code
        ##status = 200
        print(response.content)
        print(status)
        if status == 200:
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f'protocolo {f_token_retorno}')
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
                                    response = requests.get(
                                        f'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoProcedimentoRealizados/{id_gerado}',
                                        headers={'Authorization': f'Bearer {tokenEntidade}'})
                                    status = response.status_code
                                    print(response.content)
                                    print(status)
                                    dados = json.loads(response.content)
                                    procedimento_id = dados['procedimento']['id']

                                    stmt = text(f"update cnv_atendprontuario set id_proced = {id_gerado} where sequenc = {id_integracao} and id_procedimento = {procedimento_id}")
                                    session.execute(stmt)
                                    session.commit()
                                if status == "ERRO":
                                    stmt = text(f"update cnv_atendprontuario set mensagemerroproc = '{mensaErro}' where sequenc = {id_integracao} and id_procedimento = {proced}")
                                    print(stmt)
                                    session.execute(stmt)
                                    session.commit()
                    except json.JSONDecodeError:
                        print("Erro: A resposta não é um JSON válido.")
                    except KeyError as e:
                        print(f"Erro: Chave ausente no JSON - {e}")
                else:
                    print(f'erro execução')
        data = []
