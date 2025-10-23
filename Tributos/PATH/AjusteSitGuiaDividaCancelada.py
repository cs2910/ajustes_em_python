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
urllac = 'https://tributos.betha.cloud/service-layer-tributos/api/lancamentos'
urlguia = 'https://tributos.betha.cloud/service-layer-tributos/api/guias'
urlmov = 'https://tributos.betha.cloud/service-layer-tributos/api/dividasMovtos'


urlPostPocur = ''
#### Token da Endidade

tokenEntidade = 'ceb53c70-6b23-48e6-875b-d32458c5f259'

'''Utilizando select direto'''

query = text("select idguia,sitguia from bthsc206231lanc where protocolo is null and sitguia != 'PAGA' group by idguia,sitguia")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 25
data = []
datalanc = []
dataguia = []
for tabela in resultados:
    situacao = tabela.sitguia
    print(f'idGuia {tabela.idguia} Situação {tabela.sitguia}')
    (data.append({
        "idIntegracao": f'{tabela.iddivida}',
        "dividasMovtos": {
            "idDividas": tabela.iddivida,
            "tiposMovimentacoesDivida": "COMENTARIO",
            "comentario": "Débitos inscritos em dívidas com situacao aberto mas se encontravam pagos em cota única, foram ajustado no atendimento do chamado BTHSC-206231"
        }
    }))
    data.append({
        "idIntegracao": f'{tabela.idguia}',
        "guias": {
            "idGerado": {
                "id": tabela.idguia
            },
            "situacao": situacao
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

        response = requests.patch(urlguia,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(data))

        status = response.status_code
        print(response.content)
        print(status)
        if status == 200:
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']

            print(f'protocolo {f_token_retorno}')
            lista_sequenc_formatada = ", ".join([f"'{item['idIntegracao']}'" for item in data])
            stmt = text(f"UPDATE bthsc206231lanc SET protocolo = '{f_token_retorno}' WHERE idguia IN ({lista_sequenc_formatada})")
            session.execute(stmt)
            session.commit()
        else:
            print(f'erro execução')
        ## Zerar lista
        data = []
        datamov = []
