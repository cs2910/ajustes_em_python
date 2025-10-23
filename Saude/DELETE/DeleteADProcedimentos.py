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

tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

'''Utilizando select direto'''

query = text("select idgerado,idprocedimento, sequenc,a.codigo from cnv_delatenddomicprocedimentos ca  "
             "inner join cnv_id_atendom a on "
             "a.id_gerado = ca.idgerado where ca.protocolo is null order by a.sequenc")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
data = []
for tabela in resultados:
    print(f'{tabela.sequenc} - {tabela.codigo} - {tabela.idgerado} - {tabela.idprocedimento}')
    data.append({
        "idIntegracao": f"{tabela.idgerado}",
        "atendimentoDomiciliarProcedimento": {
            "idGerado": tabela.idprocedimento
        }
    })

    ##print(len(data))
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

        response = requests.delete(urlPost,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(data))
        status = response.status_code
        print(response.content)
        ##status = 200
        if status == 200:
    ##        print("entrou")
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f'protocolo {f_token_retorno}')
            ##f_token_retorno = '68c1588430768a017408e351'
            lista_de_pares = [
                (
                    item['idIntegracao'],
                    item['atendimentoDomiciliarProcedimento']['idGerado']
                )
                for item in data
            ]

            valores_formatados = [
                f"('{idgerado}', {idprocedimento})"
                for idgerado, idprocedimento in lista_de_pares
            ]
            valores_in_sql = ", ".join(valores_formatados)
            stmt = text(f"""
                        UPDATE cnv_delatenddomicprocedimentos 
                        SET protocolo = '{f_token_retorno}' 
                        WHERE (idgerado, idprocedimento) IN ({valores_in_sql})
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
    response = requests.delete(urlPost,
                             headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                             data=json.dumps(data))
    status = response.status_code
    print(response.content)
    print(status)
    if status == 200:
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['idLote']
        print(f'protocolo {f_token_retorno}')
        lista_de_pares = [
            (
                item['idIntegracao'],
                item['atendimentoDomiciliarProcedimento']['idGerado']
            )
            for item in data
        ]

        valores_formatados = [
            f"('{idgerado}', {idprocedimento})"
            for idgerado, idprocedimento in lista_de_pares
        ]
        valores_in_sql = ", ".join(valores_formatados)
        stmt = text(f"""
                    UPDATE cnv_delatenddomicprocedimentos 
                    SET protocolo = '{f_token_retorno}' 
                    WHERE (idgerado, idprocedimento) IN ({valores_in_sql})
                    """)
        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')
    ## Zerar lista
