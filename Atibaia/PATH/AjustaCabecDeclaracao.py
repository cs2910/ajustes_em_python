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
urlPost = 'https://livroeletronico.betha.cloud/livro-eletronico2/service-layer-livro/api/declaracoes'
#### Token da Endidade

tokenEntidade = '0248657a-d5b1-4e87-9b3f-72097c79dc7a'
##tokenEntidade = ''


'''Utilizando select direto'''

query = text("select * from ajuscabecdecl where protocolo is null")

##query = text("select * from ecocep where novocep is not null and codigo = '1'")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
data = []
for tabela in resultados:
    print(f'{tabela.declaracao} - {tabela.vlrtotal}- {tabela.vlrtotaliss}- {tabela.vlrliquido}- {tabela.vlrliquidoiss}')
    data.append({
        "idIntegracao": f"{tabela.declaracao}",
        "declaracoes": {
            "idGerado": {
                "iEntidades": 11229,
                "iDeclaracoes": tabela.declaracao
            },
            "vlBaseCalculo": tabela.vlrliquido,
            "vlImposto": tabela.vlrliquidoiss
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
        if status == 200:
    ##        print("entrou")
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f'protocolo {f_token_retorno}')

            for item in data:
                stmt = text(f"UPDATE ajuscabecdecl SET protocolo = '{f_token_retorno}' WHERE  declaracao  = '{item['declaracoes']['idGerado']['iDeclaracoes']}'")
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
    response = requests.patch(urlPost,
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
            stmt = text(f"UPDATE ajuscabecdecl SET protocolo = '{f_token_retorno}' WHERE  declaracao  = '{item['declaracoes']['idGerado']['iDeclaracoes']}'")
            session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')