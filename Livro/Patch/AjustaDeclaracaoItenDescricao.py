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
urlPost = 'https://livroeletronico.betha.cloud/livro-eletronico2/service-layer-livro/api/declaracoesdfitens'
#### Token da Endidade

tokenEntidade = '5cbc551b-267e-4041-8df3-03ebc9b7e0be'
##tokenEntidade = ''


'''Utilizando select direto'''

query = text("select df.entidade,df.declaracao,df.documentos,discriminacao,ddi.listaservico,ddi.seq from notafiscalcloud n "
             "join itennfcloud inf on inf.id_notas_fiscais = n.id "
             "join pessoascloud p on p.id = n.id_pessoas "
             "join pessoaslivro p2 on p2.cnpj = p.inscricao "
             "join declaracoes_df df on "
             "  df.contribuinte  = p2.id and"
             "  df.documentos = n.nro_nota "
             "join declaracoes_df_itens ddi on "
             "  ddi.entidade = df.entidade and"
             "  ddi.declaracoes = df.declaracao and "
             "  ddi.documentos = df.documentos  "
             "where "
             "ddi.descservico is null and n.id_pessoas = 2394165 and "
             "inf.discriminacao is not null and " 
             "df.documentos = 230")

##query = text("select * from ecocep where novocep is not null and codigo = '1'")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 1
data = []
for tabela in resultados:
    print(f'Entidade {tabela.entidade} Declaração {tabela.declaracao} NF {tabela.documentos}')
    descricao = tabela.discriminacao
    data.append({
        "idIntegracao": f"{tabela.documentos}",
        "declaracoesDfItens": {
            "idGerado": {
                "iEntidades": tabela.entidade,
                "iDeclaracoes": tabela.declaracao,
                "iDocumentos": tabela.documentos,
                "iListasServicos": f"{tabela.listaservico}",
                "iSequencias": tabela.seq
            },
            "descricaoServico": descricao
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
                    item["declaracoesDfItens"]["idGerado"]["iEntidades"],
                    item["declaracoesDfItens"]["idGerado"]["iDeclaracoes"],
                    item["declaracoesDfItens"]["idGerado"]["iDocumentos"]
                )
                for item in data
            ]
            valores_formatados = [
                f"('{iEntidades}', {iDeclaracoes},{iDocumentos})"
                for iEntidades, iDeclaracoes,iDocumentos in lista_de_pares
            ]
            valores_in_sql = ", ".join(valores_formatados)
            stmt = text(f"""
                            UPDATE declaracoes_df_itens 
                            SET protocolodescr = '{f_token_retorno}' 
                            WHERE (entidade, declaracoes,documentos) IN ({valores_in_sql})
                        """)
            session.execute(stmt)
            session.commit()
            print(f'protocolo {f_token_retorno}')
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
                item["declaracoesDfItens"]["idGerado"]["iEntidades"],
                item["declaracoesDfItens"]["idGerado"]["iDeclaracoes"],
                item["declaracoesDfItens"]["idGerado"]["iDocumentos"]
            )
            for item in data
        ]
        valores_formatados = [
            f"('{iEntidades}', {iDeclaracoes},{iDocumentos})"
            for iEntidades, iDeclaracoes, iDocumentos in lista_de_pares
        ]
        valores_in_sql = ", ".join(valores_formatados)
        stmt = text(f"""
                                        UPDATE declaracoes_df_itens 
                                        SET protocolodescr = '{f_token_retorno}' 
                                        WHERE (entidade, declaracao,documentos) IN ({valores_in_sql})
                                    """)
        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')