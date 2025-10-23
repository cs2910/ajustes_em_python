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

tokenEntidade = '94c4ea28-76c5-4e34-94ba-165c1ce7a632'
##tokenEntidade = ''

ano = 2025
vigInicial = f'{ano}-01-01'
vigFinal = f'{ano}-12-31'

##tabelas = session.query(Benparcial).all()
##tabelas = session.query(Benparcial).where(Benparcial.id == '1')
'''Utilizando select direto'''
query = text("SELECT * FROM imovelresp where id < 5")
resultados = session.execute(query).fetchall()

for registro in resultados:
    print(registro.id)


##tabelas = session.query(Benparcial, Idimoresp).join(Idimoresp, Benparcial.idimovel == Idimoresp.idimovel).where(
tabelas = session.query(Benparcial).order_by(Benparcial.id.asc()).where(
    Benparcial.id >= 1,Benparcial.id <= 6,
    ano >= extract('year', Benparcial.dtinicial),
    ano <= extract('year', Benparcial.dtfinal),
)
data = []
for tabela in tabelas:
##for tabela, imovel in tabelas:

    data_inicial = tabela.dtinicial.date()
    data_inicial = data_inicial.strftime('%d/%m/%Y')
    data_final = tabela.dtfinal.date()
    data_final = data_final.strftime('%d/%m/%Y')

    tipoPerc = tabela.codtipo
    if(tipoPerc == 40 or tipoPerc == 104 or tipoPerc == 107):
        percent = 100
    else:
        percent = tabela.percdesconto
    obs = f'Periodo {data_inicial} ate {data_final}, Processo {tabela.numprocesso}, percentual de {percent}%'
    idTabela = tabela.id
    print(tabela.idresp)
    print(tabela.dtinicial)
    print(tabela.dtfinal)
    data.append({ "idIntegracao": f'{idTabela}',
             "manutencoesCalculos":
                 {
                     "abrangencia": "INDIVIDUAL",
                     "anoVigencia": ano,
                     "idBeneficioFiscal": tabela.idbeneficio,
                     "dtRequerimento": f'{tabela.dtalteracao.date()}',
                     "justificativa": f'{obs}',
                     "nroProcesso": f'{tabela.numprocesso}',
                     "observacao": f'{obs}',
                     "processandoReferentes": "SIM",
                     "idRequerente": tabela.idresp,
                     "situacao": "DEFERIDO",
                     "tipoRequerimento": "EXTERNO",
                     "tipoSolicitacao": "ISENCAO",
                     "tipoVigencia": "ANO",
                     "percLancadoReq": percent,
                     "percLancado": percent,
                 }
             })
    print(data)

for i in range(0, len(data), 3):
    subset = data[i:i+3]
    dados = json.dumps(subset, ensure_ascii=False, indent=4)
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
        stmt = Protbeneficio(protocolo=f_token_retorno)
        print(stmt)
        session.add(stmt)  ##f_situacao = dadosGet['situacao']
        session.commit()
    else:
        stmt = Protbeneficio(protocolo='erro')
        print(stmt)
        session.add(stmt)  ##f_situacao = dadosGet['situacao']
        session.commit()

