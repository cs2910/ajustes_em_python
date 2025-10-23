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
urlPost = 'https://procuradoria.betha.cloud/service-layer-procuradoria/api/pessoasEnderecos/'
#### Token da Endidade

tokenEntidade = 'cd207fc9-18dc-404f-aff7-66801e120654'
##tokenEntidade = ''


'''Utilizando select direto'''
##tabelas = session.query(Pensend).where(Pensend.id == '15205509')
query = text("select * from procpescep where novocep is not null and  novocep != '11111111' and protocolo is null ")
##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()

for tabela in resultados:
    print(f'{tabela.id} - {tabela.nmpessoa}')
    print(tabela.nomelogra)
    print(f'{tabela.ceps} - {tabela.novocep}')
    data = []
    data.append({"idIntegracao": f'{tabela.id}',
                 "pessoasEnderecos": {
                     "idGerado": {
                         "id": tabela.id
                     },
                     "cep": tabela.novocep,
                 }})

##    print(data)
    dados = json.dumps(data)
    print(dados)
    response = requests.patch(urlPost,
                             headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                             data=dados)
    status = response.status_code
    print(response.content)
    print(status)
    if status == 200:
##        print("entrou")
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['idLote']
        print(f'protocolo {f_token_retorno}')
        stmt = text(f"UPDATE procpescep SET protocolo = '{f_token_retorno}' WHERE id = '{tabela.id}'")
##        print(stmt)
        result = session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')

