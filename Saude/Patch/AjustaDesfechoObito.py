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
##urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDesfechos'
#### Token da Endidade

tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'


'''Utilizando select direto'''

query = text("select d.id_gerado as iddesfecho,c.sequenc,desfecho,id_profissional,id_cbo,id_especialidade,c.codigo,c.id_gerado "
             "from cnv_antenddesfecho d "
             "inner join cnv_id_atendom c on "
             "c.sequenc = d.sequenc  "
             "where desfecho = 4 and d.id_gerado is not null")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
offset = 0
limit = 1
data = []
for tabela in resultados:
    print(f'{tabela.sequenc} - {tabela.codigo} - {tabela.id_gerado} - Desfecho {tabela.desfecho} - {tabela.iddesfecho}')
    f_token_retorno = {tabela.iddesfecho}
    params = {'offset': offset, 'limit': limit}
    responseget = requests.get(
        f'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDesfechos/{tabela.iddesfecho}',
        headers={'Authorization': f'Bearer 38096b6b-d3b5-4895-8c0b-92d355c7dee3'},
        params=params)

    status = responseget.status_code
    print(responseget.content)
    print(status)
    if status == 200:
        recurrence = json.loads(responseget.content)
        desfecho = recurrence.get('desfechoPersonalizado')
        if desfecho:
            desfecho_id = desfecho.get('id')
        print(desfecho_id)
        if desfecho_id != 7:
            data.append({
                "idIntegracao": f'{tabela.sequenc}',
                "atendimentoDesfecho": {
                    "idGerado": tabela.iddesfecho,
                    "desfechoPersonalizado": {
                        "id": 7
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

            response = requests.post(urlPost,
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
                stmt = text(f"UPDATE cnv_antenddesfecho SET protocoloobito = '{f_token_retorno}' WHERE sequenc IN ({lista_sequenc_formatada})")
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
    response = requests.post(urlPost,
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
        stmt = text(
            f"UPDATE cnv_antenddesfecho SET protocoloobito = '{f_token_retorno}' WHERE sequenc IN ({lista_sequenc_formatada})")
        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')