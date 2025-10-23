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

tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'


'''Utilizando select direto'''

query = text("select sequenc, desfecho ,id_gerado from public.cnv_antenddesfecho where id_gerado is not null ")
            ## "and sequenc in (6,7,11)")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
offset = 0
limit = 1
data = []
print(f"Iniciando a consulta de {len(resultados)} registros")
for tabela in resultados:
    f_token_retorno = {tabela.id_gerado}
    params = {'offset': offset, 'limit': limit}
    responseget = requests.get(
        f'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDesfechos/{tabela.id_gerado}',
        headers={'Authorization': f'Bearer {tokenEntidade}'})

    status = responseget.status_code
    ##print(responseget.content)
    ##print(status)
    if status == 200:
        recurrence = json.loads(responseget.content)
        desfecho = recurrence.get('desfechoPersonalizado')
        if desfecho is None:
            print(f'{tabela.sequenc} - {tabela.id_gerado} - Desfecho {tabela.desfecho}')
            if tabela.desfecho == 1:
                iddesfecho = 11
            elif tabela.desfecho == 2:
                iddesfecho = 10
            elif tabela.desfecho == 3:
                iddesfecho = 1
            elif tabela.desfecho == 4:
                iddesfecho = 7
            elif tabela.desfecho in (5, 6, 7):
                iddesfecho = 4

            print(iddesfecho)
            if iddesfecho:
                data.append({
                    "idIntegracao": f'{tabela.sequenc}',
                    "atendimentoDesfecho": {
                        "idGerado": tabela.id_gerado,
                        "desfechoPersonalizado": {
                            "id": iddesfecho
                        },
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

            response = requests.patch(urlPost,
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
                stmt = text(f"UPDATE cnv_antenddesfecho SET protocolopatch = '{f_token_retorno}' WHERE sequenc IN ({lista_sequenc_formatada})")
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
    response = requests.patch(urlPost,
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
        stmt = text(f"UPDATE cnv_antenddesfecho SET protocolopatch = '{f_token_retorno}' WHERE sequenc IN ({lista_sequenc_formatada})")
        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')