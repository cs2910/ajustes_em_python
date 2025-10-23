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

tokenEntidade = 'fe8704a7-f0da-41f9-91d1-acf75a1d5a0f'

'''Utilizando select direto'''

query = text("select a.sequenc, ap.id_profissional, ap.id_procedimento, ap.id_especialidade, ap.id_cbo, ap.id_cidadao, a.id_gerado as idatendimentos, cad.dataatendimento from public.cnv_atendprontuario ap "
             "inner join cnv_atendimento a on "
             "a.sequenc = ap.sequenc "
             "inner join cnv_atend_domiciliar cad on "
             "cad.sequenc = a.sequenc "
             "where id_proced is null and a.sequenc != 376019 and mensagemerroproc ilike '%CBO%'"
             "order by a.sequenc")

resultados = session.execute(query).fetchall()
batch_size = 1
data = []
for tabela in resultados:
    if tabela.id_procedimento == 51462:
        cbo = 242
    elif tabela.id_procedimento == 46108:
        cbo = 3168
    elif tabela.id_procedimento == 46109:
        cbo = 839
    elif tabela.id_procedimento == 46198:
        cbo = 839
    elif tabela.id_procedimento in(46246,4628746312,46463,46583,46912):
        cbo = 1610
    elif tabela.id_procedimento == 47114:
        cbo = 3168
    elif tabela.id_procedimento in (47121,47130):
        cbo = 839
    elif tabela.id_procedimento == 47134:
        cbo = 1608
    elif tabela.id_procedimento == 51472:
        cbo = 839
    elif tabela.id_procedimento == 47159:
        cbo = 178
    elif tabela.id_procedimento == 47160:
        cbo = 839
    elif tabela.id_procedimento == 47166:
        cbo = 1623
    elif tabela.id_procedimento in (50854,50859,47178,47182):
        cbo = 1610
    elif tabela.id_procedimento == 47183:
        cbo = 178
    elif tabela.id_procedimento in (47211,47212,47213):
        cbo = 839
    elif tabela.id_procedimento in (47214,47215):
        cbo = 1610
    elif tabela.id_procedimento in (47217,47222,47223):
        cbo = 3168
    elif tabela.id_procedimento == 47225:
        cbo = 839
    elif tabela.id_procedimento == 47226:
        cbo = 3168
    elif tabela.id_procedimento in (51476,51477,51482,51484, 51485):
        cbo = 839
    elif tabela.id_procedimento == 47244:
        cbo = 1021
    elif tabela.id_procedimento in (47307,47310,47313,47443, 47461, 47493):
        cbo = 1610
    elif tabela.id_procedimento == 47707:
        cbo = 420
    elif tabela.id_procedimento in (47728,47739, 47777, 49145):
        cbo = 1623
    elif tabela.id_procedimento in (47972,50382,48808,48822):
        cbo = 1610


    print(f'{tabela.sequenc} -  {tabela.id_procedimento} - Id {tabela.idatendimentos}')
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
                "id": cbo
            },
            "cliente":{
                "id": tabela.id_cidadao
            },
            "dataHoraRegistro":"",
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
    ##        print("entrou")
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f'protocolo {f_token_retorno}')
            lista_de_pares = [
                (
                    item['idIntegracao'],
                    item['atendimentoProcedimentoRealizado']['procedimento']['id']
                )
                for item in data
            ]

            # 2. Constrói e executa um ÚNICO comando UPDATE em lote
            # Isso atualiza todos os registros de uma vez, melhorando a velocidade drasticamente
            valores_formatados = [
                f"('{sequenc}', {id_procedimento})"
                for sequenc, id_procedimento in lista_de_pares
            ]
            valores_in_sql = ", ".join(valores_formatados)

            stmt = text(f"""
                    UPDATE cnv_atendprontuario 
                    SET protocoloprocante = '{f_token_retorno}' 
                    WHERE (sequenc, id_procedimento) IN ({valores_in_sql})
                """)

            session.execute(stmt)
            session.commit()
        else:
            print(f'erro execução')
        ## Zerar lista
        data = []
if data:
    print(data)
    dados = json.dumps(data)
    print(dados)
    response = requests.post(urlPost,
                             headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                             data=json.dumps(data))
    status = response.status_code
    print(response.content)
    print(status)
    if status == 200:
        ##        print("entrou")
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['idLote']
        print(f'protocolo {f_token_retorno}')
        lista_de_pares = [
            (
                item['idIntegracao'],
                item['atendimentoProcedimentoRealizado']['procedimento']['id']
            )
            for item in data
        ]

        # 2. Constrói e executa um ÚNICO comando UPDATE em lote
        # Isso atualiza todos os registros de uma vez, melhorando a velocidade drasticamente
        valores_formatados = [
            f"('{sequenc}', {id_procedimento})"
            for sequenc, id_procedimento in lista_de_pares
        ]
        valores_in_sql = ", ".join(valores_formatados)

        stmt = text(f"""
                UPDATE cnv_atendprontuario 
                SET protocoloprocante = '{f_token_retorno}' 
                WHERE (sequenc, id_procedimento) IN ({valores_in_sql})
            """)

        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')