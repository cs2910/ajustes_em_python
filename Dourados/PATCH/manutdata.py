from sqlalchemy.orm import sessionmaker
from sqlalchemy import extract, update,insert, text
from tabelatributos import Benparcial, Idimoresp, Protbeneficio
from conexao import engine, connection
import requests, json

#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Linka
urlPost = 'https://tributos.betha.cloud/service-layer-tributos/api/manutencoesCalculos/'
#### Token da Endidade

tokenEntidade = 'c668172c-0f6e-4917-900d-337e636a2821'
##tokenEntidade = ''

vigInicial = f'2024-12-12'


'''Utilizando select direto'''
###query = text("SELECT * FROM ajustedtisencao where idmanut = 177860555")
query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()

for registro in resultados:
    print(registro.idmanut)
    data = []
    data.append({"idIntegracao": f'{registro.idmanut}',
                 "manutencoesCalculos": {
                     "idGerado": {
                         "id": registro.idmanut
                     },
                     "dtIniVigencia": "2024-12-12",
                 }})

    print(data)
    dados = json.dumps(data)
    print(dados)
    response = requests.patch(urlPost,
                             headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                             data=dados)
    status = response.status_code
    print(response.content)
    print(status)
    if status == 200:
        print("entrou")
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['idLote']
        print(f'protocolo {f_token_retorno}')
        stmt = text(f"UPDATE ajustedtisencao SET protocolo = '{f_token_retorno}' WHERE idmanut = {registro.idmanut}")
        print(stmt)
        result = session.execute(stmt)
        session.commit()
    else:
        stmt = Protbeneficio(protocolo='erro')
        print(stmt)

