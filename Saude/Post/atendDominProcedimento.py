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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDomiciliarProcedimentos'
#### Token da Endidade

tokenEntidade = 'ba7177f4-8317-4adb-9e35-3edeca0e5306'

'''Utilizando select direto'''

query = text("select p.sequenc, a.id_gerado , a.codigo,id_procedimento,id_profissional,"
             "id_especialidade,id_cbo,id_cidadao from cnv_atendprontuario p "
             "INNER join cnv_id_atendom a ON a.sequenc = p.sequenc "
             "where p.protocolo is null  and a.id_gerado is not null and id_procedimento is not null "
             "group by p.sequenc, a.id_gerado , a.codigo,id_procedimento,id_profissional,"
             "id_especialidade,id_cbo,id_cidadao")

resultados = session.execute(query).fetchall()
batch_size = 50
data = []
for tabela in resultados:
    print(f'{tabela.sequenc} - {tabela.codigo} - {tabela.id_gerado} - {tabela.id_procedimento}')
    data.append({
        "idIntegracao": f"{tabela.sequenc}",
        "atendimentoDomiciliarProcedimento": {
            "atendimentoDomiciliar":{
                "id":  tabela.id_gerado
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
    })

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

            '''for item in data:
                ##print (item['atendimentoDomiciliarProcedimento']['procedimento']['id'])
                stmt = text(f"UPDATE cnv_atendprontuario SET protocolo = '{f_token_retorno}' WHERE  sequenc = '{item['idIntegracao']}' and id_procedimento = {item['atendimentoDomiciliarProcedimento']['procedimento']['id']}")
                session.execute(stmt)
            session.commit()'''
            lista_de_pares = [
                (
                    item['idIntegracao'],
                    item['atendimentoDomiciliarProcedimento']['procedimento']['id']
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
                    SET protocolo = '{f_token_retorno}' 
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
                item['atendimentoDomiciliarProcedimento']['procedimento']['id']
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
                SET protocolo = '{f_token_retorno}' 
                WHERE (sequenc, id_procedimento) IN ({valores_in_sql})
            """)

        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')