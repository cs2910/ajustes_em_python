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
urlPost = 'https://tributos.betha.cloud/service-layer-tributos/api/dividas'
urlMov = 'https://tributos.betha.cloud/service-layer-tributos/api/dividasMovtos'


urlPostPocur = ''
#### Token da Endidade

tokenEntidade = 'ceb53c70-6b23-48e6-875b-d32458c5f259'

'''Utilizando select direto'''

query = text("SELECT * FROM bthsc206231 where  protocolo is null")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
data = []
datamov = []
dataProc = []
for tabela in resultados:
    situacao = 'CANCELADA'
    if tabela.situacaoparc == 'ABERTO':
        situacao = 'PAGA_PARCELAMENTO'
    print(f'{tabela.codigo} - idDivida {tabela.id_gerado} idProcuradoria {tabela.idprocuradoria} ')
    data.append({
        "idIntegracao": f'{tabela.codigo}',
        "dividas": {
            "idGerado": {
                "id": tabela.id_gerado
            },
            "situacaoDivida": situacao
        }
    })
    datamov.append({
        "idIntegracao": f'{tabela.codigo}',
        "dividasMovtos": {
            "idDividas": tabela.id_gerado,
            "tiposMovimentacoesDivida": "COMENTARIO",
            "comentario":"Alteração da situação da dívida referente ao chamado BTHSC-206231"
       }
    })
    ##print(len(data))
    if len(data) == batch_size:
        print(data)
    ##    dados = json.dumps(data, ensure_ascii=False)
        try:
            dados = json.dumps(data, ensure_ascii=False) ## , indent=4 mostra o json em arvore
            dadosmov = json.dumps(datamov, ensure_ascii=False)
            print("Conversão para JSON bem-sucedida!")
            print(dados)
            print(dadosmov)
        except TypeError as e:
            print(f"Ocorreu um erro: {e}")

        response = requests.patch(urlPost,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(data))

        responseMov = requests.post(urlMov,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(datamov))

        status = response.status_code
        status_mov = responseMov.status_code
        print(response.content)
        print(status)
        print(responseMov.content)
        print(status_mov)
        if status == 200:
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f'protocolo {f_token_retorno}')
            lista_sequenc_formatada = ", ".join([f"'{item['idIntegracao']}'" for item in data])
            stmt = text(f"UPDATE bthsc206231 SET protocolo = '{f_token_retorno}' WHERE codigo IN ({lista_sequenc_formatada})")
            session.execute(stmt)
            session.commit()
        else:
            print(f'erro execução')
        ## Zerar lista
        data = []
        datamov = []
if data:
    print(data)
    dados = json.dumps(data)
    print(dados)
    response = requests.patch(urlPost,
                              headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                              data=json.dumps(data))

    responseMov = requests.post(urlMov,
                                headers={'Authorization': f'Bearer {tokenEntidade}',
                                         'content-type': 'application/json'},
                                data=json.dumps(datamov))
    status = response.status_code
    status_mov = responseMov.status_code
    print(response.content)
    print(status)
    print(responseMov.content)
    print(status_mov)
    if status == 200:
        ##        print("entrou")
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['idLote']
        print(f'protocolo {f_token_retorno}')
        lista_sequenc_formatada = ", ".join([f"'{item['idIntegracao']}'" for item in data])
        stmt = text(f"UPDATE bthsc206231 SET protocolo = '{f_token_retorno}' WHERE codigo IN ({lista_sequenc_formatada})")
        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')