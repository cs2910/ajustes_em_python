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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDesfechos'
#### Token da Endidade

tokenEntidade = 'fe8704a7-f0da-41f9-91d1-acf75a1d5a0f'

'''Utilizando select direto'''

query = text("select * from cnv_antenddesfecho ad "
             "inner join cnv_id_atendom a on a.sequenc = ad.sequenc "
             "where ad.sequenc > 538541")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
iddesfecho = 0
cids = []
data = []
for tabela in resultados:
    print(f'{tabela.sequenc} - {tabela.codigo} - {tabela.id_gerado}')
    if tabela.desfecho == 1:
        iddesfecho = 11
    elif tabela.desfecho == 2:
        iddesfecho = 10
    elif tabela.desfecho == 3:
        iddesfecho = 1
    elif tabela.desfecho == 4:
        iddesfecho = 7
    elif tabela.desfecho in (5,6,7):
        iddesfecho = 4


    data.append({
        "idIntegracao": f'{tabela.sequenc}',
        "atendimentoDesfecho": {
            "desfechoPersonalizado": {
                "id": iddesfecho
            },
            "profissional": {
                "id": tabela.id_profissional,
            },
            "especialidade": {
                "id": tabela.id_especialidade,
            },
            "cbo": {
                "id": tabela.id_cbo,
            }
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
            lista_sequenc_formatada = ", ".join([f"'{item['idIntegracao']}'" for item in data])
            stmt = text(f"UPDATE cnv_antenddesfecho SET protocolo = '{f_token_retorno}' WHERE sequenc IN ({lista_sequenc_formatada})")
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
        lista_sequenc_formatada = ", ".join([f"'{item['idIntegracao']}'" for item in data])
        stmt = text(
            f"UPDATE cnv_antenddesfecho SET protocolo = '{f_token_retorno}' WHERE sequenc IN ({lista_sequenc_formatada})")
        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')