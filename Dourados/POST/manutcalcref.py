from sqlalchemy.orm import sessionmaker
from sqlalchemy import extract, update
from tabelatributos import Benparcial, Idimoresp
from conexao import engine, connection
import requests, json

#################################################
#           Conexão com banc de dados           #
#################################################
Session = sessionmaker(bind=engine)
session = Session()

#### Linka
urlPost = 'https://tributos.betha.cloud/service-layer-tributos/api/manutCalcReferentes/'
#### Token da Endidade

tokenEntidade = 'ec6f1132-4a10-49b2-9457-e88c3e7a6aad'
##tokenEntidade = ''

ano = 2025
vigInicial = f'{ano}-01-01'
vigFinal = f'{ano}-12-31'

##tabelas = session.query(Benparcial).all()
##tabelas = session.query(Benparcial).where(Benparcial.id == '1')

##tabelas = session.query(Benparcial, Idimoresp).join(Idimoresp, Benparcial.idimovel == Idimoresp.idimovel).where(
tabelas = session.query(Benparcial).where(
    Benparcial.id > 4,
    Benparcial.idcloud.isnot(None),
    ano >= extract('year', Benparcial.dtinicial),
    ano <= extract('year', Benparcial.dtfinal),
)
for tabela in tabelas:
##for tabela, imovel in tabelas:
    idTabela = tabela.id
    data = [{
        "idIntegracao": "INTEGRACAO1",
        "manutCalcReferentes": {
            "idManutencoesCalculos": tabela.idcloud,
            "idImoveis": tabela.idimovel,
            "idPessoas": tabela.idresp
        }
    }]
    dados = json.dumps(data)
    print(dados)
    response = requests.post(urlPost,
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
        '''Update do Protocolo'''
        stmt = (update(Benparcial).where(Benparcial.id == idTabela).values(protrocref=f_token_retorno))
        print(stmt)
        session.execute(stmt)  ##f_situacao = dadosGet['situacao']
        session.commit()
    else:
        '''Update do Protocolo'''
        stmt = (update(Benparcial).where(Benparcial.id == idTabela).values(protrocref='erro'))
        print(stmt)
        session.execute(stmt)  ##f_situacao = dadosGet['situacao']
        session.commit()