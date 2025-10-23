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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoProcedimentoRealizados'
#### Token da Endidade

tokenEntidade = 'fe8704a7-f0da-41f9-91d1-acf75a1d5a0f'

'''Utilizando select direto'''

query = text("select a.sequenc,cia.codigo, id_proced as id_gerado, cad.dataatendimento from public.cnv_atendprontuario ap "
             "inner join cnv_atendimento a on "
             "a.sequenc = ap.sequenc "
             "inner join cnv_atend_domiciliar cad on "
             "cad.sequenc = a.sequenc "
             "inner join cnv_id_atendom cia on "
             "cia.sequenc = cad.sequenc "
             "where id_proced is not null and cia.sequenc = 376019 "
             "order by a.sequenc")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 1
data = []
for tabela in resultados:
    print(f'{tabela.sequenc} - {tabela.codigo} - {tabela.id_gerado} - {tabela.dataatendimento}')
    data.append({
        "idIntegracao": f"{tabela.sequenc}",
        "atendimentoProcedimentoRealizado": {
            "idGerado": tabela.id_gerado,
            "dataHoraRegistro": f"{tabela.dataatendimento} 10:00:00"
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
        print(f'Status do patch: {status}')
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
