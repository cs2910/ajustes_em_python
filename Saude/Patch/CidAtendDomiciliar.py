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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentosDomiciliares'
#### Token da Endidade

tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

'''Utilizando select direto'''

query = text("select a.sequenc,codigo, id_gerado,ds_filtro_cids  from cnv_atend_domiciliar d "
             "inner join cnv_id_atendom a on "
             "a.sequenc = d.sequenc "
             "where ds_filtro_cids not ilike '%||%'")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
cids = []
data = []
for tabela in resultados:
    print(f'{tabela.sequenc} - {tabela.codigo} - {tabela.id_gerado} - {tabela.ds_filtro_cids}')
    data.append({
        "idIntegracao": f"{tabela.sequenc}",
        "atendimentoDomiciliar": {
            "idGerado": tabela.id_gerado,
            "atendimentoDomiciliarAvaliarElegibilidade": {}
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
        if status == 200:
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f"Protocolo gerado {f_token_retorno}")
        data = []
if data:
    print(data)
    dados = json.dumps(data)
    print(dados)
    response = requests.post(urlPost,
                             headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                             data=json.dumps(data))
    status = response.status_code
    if status == 200:
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence['idLote']
        print(f"Protocolo gerado {f_token_retorno}")
