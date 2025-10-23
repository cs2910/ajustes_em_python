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
urlPost = 'https://procuradoria.betha.cloud/service-layer-procuradoria/api/dividas'
#### Token da Endidade
tokenEntidade = 'ceb53c70-6b23-48e6-875b-d32458c5f259'

'''Utilizando select direto'''

query = text("SELECT * FROM bthsc206231 where  idprocuradoria is not null and protocoloproc is null")
resultados = session.execute(query).fetchall()
batch_size = 50
data = []
for tabela in resultados:
    situacao = 'C'
    print(f'{tabela.codigo} - idDivida {tabela.id_gerado} idProcuradoria {tabela.idprocuradoria} ')
    data.append({
        "idIntegracao": f'{tabela.codigo}',
        "dividas": {
            "idGerado": {
                "id": tabela.idprocuradoria
            },
            "situacaoDivida": situacao
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
            lista_sequenc_formatada = ", ".join([f"'{item['idIntegracao']}'" for item in data])
            stmt = text(f"UPDATE bthsc206231 SET protocoloproc = '{f_token_retorno}' WHERE codigo IN ({lista_sequenc_formatada})")
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
        lista_sequenc_formatada = ", ".join([f"'{item['idIntegracao']}'" for item in data])
        stmt = text(f"UPDATE bthsc206231 SET protocoloproc = '{f_token_retorno}' WHERE codigo IN ({lista_sequenc_formatada})")
        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')