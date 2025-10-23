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


urlPostPocur = ''
#### Token da Endidade

tokenEntidade = 'ceb53c70-6b23-48e6-875b-d32458c5f259'

'''Utilizando select direto'''

query = text("SELECT * FROM bthsc218848 where protocolo is null")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 49
data = []
datalanc = []
dataguia = []
for tabela in resultados:
    situacao = 'CANCELADA'
    print(f'{tabela.codigo} - idDivida {tabela.iddivida} idLanc {tabela.idlancamento} idGuia {tabela.idguia}')
    data.append({
        "idIntegracao": f'{tabela.codigo}',
        "dividas": {
            "idGerado": {
                "id": tabela.iddivida
            },
            "situacaoDivida": situacao
        }
    })
    datalanc.append({
        "idIntegracao": f'{tabela.codigo}',
        "lancamentos": {
            "idGerado": {
                "id": tabela.idlancamento
            },
            "situacao": "ABERTO"
        }
    })
    dataguia.append({
        "idIntegracao": f'{tabela.codigo}',
        "guias": {
            "idGerado": {
                "id": tabela.idguia
            },
            "situacao": "ABERTO"
        }
    })


    ##print(len(data))
    if len(data) == batch_size:
        print(data)
    ##    dados = json.dumps(data, ensure_ascii=False)
        try:
            dados = json.dumps(data, ensure_ascii=False) ## , indent=4 mostra o json em arvore
            dadoslanc = json.dumps(datalanc, ensure_ascii=False)
            dadosgui = json.dumps(dataguia, ensure_ascii=False)
            print("Conversão para JSON bem-sucedida!")
            print(dados)
            print(dadoslanc)
            print(dadosgui)
        except TypeError as e:
            print(f"Ocorreu um erro: {e}")

        response = requests.patch(urlPost,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(data))

        responselanc = requests.patch(urllac,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(datalanc))

        responseguia = requests.patch(urlguia,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(dataguia))

        status = response.status_code
        statuslanc = responselanc.status_code
        statusguia = responseguia.status_code
        print(response.content)
        print(status)
        print(responselanc.content)
        print(statuslanc)
        print(responseguia.content)
        print(statusguia)
        if status == 200:
            recurrence = json.loads(response.content)
            recurrencelanc = json.loads(responselanc.content)
            recurrenceguia = json.loads(responseguia.content)
            f_token_retorno = recurrence['idLote']
            f_token_retornolanc = recurrencelanc['idLote']
            f_token_retornoguia = recurrenceguia['idLote']

            print(f'protocolo {f_token_retorno}')
            lista_sequenc_formatada = ", ".join([f"'{item['idIntegracao']}'" for item in data])
            stmt = text(f"UPDATE bthsc218848 SET protocolo = '{f_token_retorno}', protocololanc = '{f_token_retornolanc}', "
                        f"protocologuia = '{f_token_retornoguia}' WHERE codigo IN ({lista_sequenc_formatada})")
            session.execute(stmt)
            session.commit()
        else:
            print(f'erro execução')
        ## Zerar lista
        data = []
        datamov = []
