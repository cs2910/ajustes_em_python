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

tokenEntidade = '0248657a-d5b1-4e87-9b3f-72097c79dc7a'
##tokenEntidade = ''


'''Utilizando select direto'''

query = text("select * from itensajustadosnovos "
             "where protocolo is null "
            ## "and inscricao = '11002675000196' and nro_nota = '133'"
             " order by inscricao,nro_nota,nmdeclaracao, seq")
##query = text("select * from ecocep where novocep is not null and codigo = '1'")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
data = []
for tabela in resultados:
    print(f'{tabela.inscricao} - {tabela.nro_nota} - {tabela.nmdeclaracao}')
    servPais = "S"
    if(tabela.cnaes == None):
        cnae = ""
    data.append({
        "idIntegracao": f'{tabela.nro_nota}',
        "declaracoesDfItens": {
            "iDeclaracoes": tabela.nmdeclaracao,
            "iDocumentos": tabela.nro_nota,
            "iListasServicos": f'{tabela.listasservicos}',
            "iSequencias": tabela.seq,
            "iCnaes": f'{tabela.cnaes}',
            "iIncentivosFiscais": "",
            "servicoNoPais": f'{servPais}',
            "descricaoServico": f'{tabela.discriminacao}',
            "qtdServico": tabela.quantidade,
            "vlUnitario": tabela.vl_servico,
            "vlServico": tabela.vl_servico,
            "vlDeducao": tabela.vl_deducao,
            "vlDescIncondicional": tabela.vl_desconto_incondicionado,
            "vlDescCondicional": tabela.vl_desconto_condicionado,
            "vlBaseCalculo": tabela.vl_base_calculo,
            "aliquota": tabela.aliquota,
            "vlIss": tabela.vl_iss,
            "vlTaxas": 0
        }
    })
    print(len(data))
    if len(data) == batch_size:
        print(data)
        dados = json.dumps(data, ensure_ascii=False)
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

            for item in data:
                stmt = text(f"UPDATE itensajustadosnovos SET protocolo = '{f_token_retorno}' WHERE  nro_nota = '{item['declaracoesDfItens']['iDocumentos']}' and nmdeclaracao = '{item['declaracoesDfItens']['iDeclaracoes']}' and seq = '{item['declaracoesDfItens']['iSequencias']}' ")
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
        for item in data:
            stmt = text(
                f"UPDATE itensajustados SET protocolo = '{f_token_retorno}' WHERE  nro_nota = '{item['declaracoesDfItens']['iDocumentos']}' and nmdeclaracao = '{item['declaracoesDfItens']['iDeclaracoes']}' and seq = '{item['declaracoesDfItens']['iSequencias']}' ")
            session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')