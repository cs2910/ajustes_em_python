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
urlPost = 'https://tributos.betha.cloud/service-layer-tributos/api/pessoasEnderecos/'
#### Token da Endidade

tokenEntidade = 'e0b366ba-6ad5-49f5-a0b5-ad6a3ddb2eab'
##tokenEntidade = ''


'''Utilizando select direto'''
##tabelas = session.query(Pensend).where(Pensend.id == '15205509')
query = text("select a.id,a.codigo, replace(l.novocep,'-','') as novocep  from projetada l "
             "inner join pesajuste a on "
             "a.codigo = l.id and "
             "a.nomelogra = l.logra and a.nmbairro  = l.bairro")
##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()

for tabela in resultados:
    print(tabela.id)
    print(f'{tabela.codigo} - {tabela.novocep}')
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
        stmt = text(f"UPDATE pesajuste SET protocolo = '{f_token_retorno}' WHERE id = '{tabela.id}'")
##        print(stmt)
        result = session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')

