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

tokenEntidade = 'fe8704a7-f0da-41f9-91d1-acf75a1d5a0f'

'''Utilizando select direto'''

query = text("select codigo,id_gerado, case when modalidade = 'AD1' then 5050 when modalidade = 'AD2' then 5051  end as modalidade from cnv_atend_domiciliar ad"
             " inner join cnv_id_atendom a on a.sequenc = ad.sequenc "
             "where a.sequenc > 538541 and id_gerado is not null")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 1
cids = []
data = []
for tabela in resultados:
    print(f' {tabela.codigo} -  {tabela.id_gerado} - {tabela.modalidade}')
    data.append({
        "idIntegracao": f"{tabela.id_gerado}",
        "atendimentoDomiciliar": {
            "idGerado": tabela.id_gerado,
            "atendimentoDomiciliarAvaliarElegibilidade": {
                "classificacaoModalidade": {
                    "id": tabela.modalidade
                }
            }
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
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence['idLote']
            print(f'protocolo {f_token_retorno}')
        else:
            print(f'erro execução')
        ## Zerar lista
        data = []
