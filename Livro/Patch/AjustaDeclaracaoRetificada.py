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
urlPost = 'https://livroeletronico.betha.cloud/livro-eletronico2/service-layer-livro/api/declaracoesdf'
#### Token da Endidade

tokenEntidade = '5cbc551b-267e-4041-8df3-03ebc9b7e0be'
##tokenEntidade = ''


'''Utilizando select direto'''

query = text("select * from declaracoes_df as d where  "
             " d.protocolo is null and d.retificadoras != '' and d.optante = 'S' and "
             "exists (select 1 from declaracoes where declarcao = cast(d.retificadoras as integer) and situacao = 'C') "
             "order by declaracao")

##query = text("select * from ecocep where novocep is not null and codigo = '1'")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 100
data = []
for tabela in resultados:
    print(f'Entidade {tabela.entidade} Declaração {tabela.declaracao} NF {tabela.documentos}')
    data.append({
        "idIntegracao": f"{tabela.documentos}",
        "declaracoesDf": {
            "idGerado": {
                "iEntidades": tabela.entidade,
                "iDeclaracoes": tabela.declaracao,
                "iDocumentos": tabela.documentos
            },
            "iRetificadoras": ""
        }
    })
    print(len(data))
    if len(data) == batch_size:
        print(data)
        dados = json.dumps(data, ensure_ascii=False)
        print(dados)
        response = requests.patch(urlPost,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(data))
        status = response.status_code
        print(response.content)
        print(status)
        ##status = 200
        if status == 200:
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            lista_de_pares = [
                (
                    item["declaracoesDf"]["idGerado"]["iEntidades"],
                    item["declaracoesDf"]["idGerado"]["iDeclaracoes"],
                    item["declaracoesDf"]["idGerado"]["iDocumentos"]
                )
                for item in data
            ]
            valores_formatados = [
                f"('{iEntidades}', {iDeclaracoes},{iDocumentos})"
                for iEntidades, iDeclaracoes,iDocumentos in lista_de_pares
            ]
            valores_in_sql = ", ".join(valores_formatados)
            stmt = text(f"""
                            UPDATE declaracoes_df 
                            SET protocolo = '{f_token_retorno}' 
                            WHERE (entidade, declaracao,documentos) IN ({valores_in_sql})
                        """)
            session.execute(stmt)
            session.commit()
            print(f'protocolo {f_token_retorno}')
            ##stmt = text(f"UPDATE declaracoes_df SET protocolo = '{f_token_retorno}' WHERE  entidade  = {tabela.entidade} and declaracao  = {tabela.declaracao} and documentos  = {tabela.documentos}")
        else:
            print(f'erro execução')
        ## Zerar lista
        data = []
if data:
    print(data)
    dados = json.dumps(data)
    print(dados)
    response = requests.patch(urlPost,
                             headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                             data=json.dumps(data))
    status = response.status_code
    print(response.content)
    print(status)
    if status == 200:
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['idLote']
        lista_de_pares = [
            (
                item["declaracoesDf"]["idGerado"]["iEntidades"],
                item["declaracoesDf"]["idGerado"]["iDeclaracoes"],
                item["declaracoesDf"]["idGerado"]["iDocumentos"]
            )
            for item in data
        ]
        valores_formatados = [
            f"('{iEntidades}', {iDeclaracoes},{iDocumentos})"
            for iEntidades, iDeclaracoes, iDocumentos in lista_de_pares
        ]
        valores_in_sql = ", ".join(valores_formatados)
        stmt = text(f"""
                                        UPDATE declaracoes_df 
                                        SET protocolo = '{f_token_retorno}' 
                                        WHERE (entidade, declaracao,documentos) IN ({valores_in_sql})
                                    """)
        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')