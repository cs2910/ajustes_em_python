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
urllac = 'https://tributos.betha.cloud/service-layer-tributos/api/lancamentos'
urllacrec = 'https://tributos.betha.cloud/service-layer-tributos/api/lancamentosReceitas'
urlguia = 'https://tributos.betha.cloud/service-layer-tributos/api/guias'
urlguiarec = 'https://tributos.betha.cloud/service-layer-tributos/api/guiasReceitas'


urlPostPocur = ''
#### Token da Endidade

tokenEntidade = '8b19d51d-684f-4b7f-889a-6e8fcd7ce3fe'

'''Utilizando select direto'''

query = text("select lanc,lancrec,guia,guiarec, contr  from bthsc218848novo where protocolo is null")

resultados = session.execute(query).fetchall()
batch_size = 4
data = []
datalanc = []
dataguia = []
dataguiarec = []

for tabela in resultados:
    print(f'{tabela.contr} - idDivida {tabela.lanc} idLanc {tabela.lancrec} idGuia {tabela.guia}')
    data.append({
        "idIntegracao": f"{tabela.lanc}",
        "lancamentos": {
            "idGerado": {
                "id": tabela.lanc
            },
            "idCreditosTributarios": 264997,
            "idReferente": tabela.contr,
            "tipoReferente": "CONTRIBUINTE"
        }
    })
    datalanc.append({
        "idIntegracao": f"{tabela.lanc}",
        "lancamentosReceitas": {
            "idGerado": {
                "id": tabela.lancrec
            },
            "idReceitasCreditos": 688972
        }
    })
    dataguia.append({
        "idIntegracao": f"{tabela.lanc}",
        "guias": {
            "idGerado": {
                "id": tabela.guia
            },
            "idCreditoTributario": 264997,
            "idReferente": tabela.contr,
            "tipoReferente": "CONTRIBUINTE",
            "origem": "LIVRO_ELETRONICO"
        }
    })

    dataguiarec.append({
        "idIntegracao": f"{tabela.lanc}",
        "guiasReceitas": {
            "idGerado": {
                "id": tabela.guiarec
            },
            "idReceitasCreditos": 688972
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
            dadosguiarec = json.dumps(dataguiarec, ensure_ascii=False)
            print("Conversão para JSON bem-sucedida!")
            print(dados)
            print(dadoslanc)
            print(dadosgui)
            print(dadosguiarec)
        except TypeError as e:
            print(f"Ocorreu um erro: {e}")
        print(json.dumps(dadosguiarec))

        response = requests.patch(urllac,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(data))

        responselanc = requests.patch(urllacrec,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(datalanc))

        responseguia = requests.patch(urlguia,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(dataguia))

        responseguiarec = requests.patch(urlguiarec,
                                 headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                                 data=json.dumps(dataguiarec))

        status = response.status_code
        statuslanc = responselanc.status_code
        statusguia = responseguia.status_code
        statusguiarec = responseguiarec.status_code
        print(response.content)
        print(status)
        print(responselanc.content)
        print(statuslanc)
        print(responseguia.content)
        print(statusguia)
        print(responseguiarec.content)
        print(statusguiarec)
        if status == 200:
            recurrence = json.loads(response.content)
            recurrencelanc = json.loads(responselanc.content)
            recurrenceguia = json.loads(responseguia.content)
            recurrenceguiarec = json.loads(responseguiarec.content)

            f_token_retorno = recurrence['idLote']
            f_token_retornolanc = recurrencelanc['idLote']
            f_token_retornoguia = recurrenceguia['idLote']
            f_token_retornoguiarec = recurrenceguiarec['idLote']

            print(f'protocolo {f_token_retorno}')
            lista_sequenc_formatada = ", ".join([f"'{item['idIntegracao']}'" for item in data])
            stmt = text(f"UPDATE bthsc218848novo SET protocolo = '{f_token_retorno}', protocololanc = '{f_token_retornolanc}', "
                        f"protocologuia = '{f_token_retornoguia}', protocologuiarec = '{f_token_retornoguiarec}' WHERE lanc IN ({lista_sequenc_formatada})")
            session.execute(stmt)
            session.commit()
        else:
            print(f'erro execução')
        ## Zerar lista
        data = []
        datamov = []
