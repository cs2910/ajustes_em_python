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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentos'
#### Token da Endidade

tokenEntidade = 'fe8704a7-f0da-41f9-91d1-acf75a1d5a0f'


'''Utilizando select direto'''

query = text("select cia.sequenc,codigo, id_cloud from cnv_ajustaatendimentos aa inner join tb_atend a on "
             "a.co_seq_atend = pk "
             "inner join tb_atend_prof f on f.co_seq_atend_prof = a.co_atend_prof "
             "inner join cnv_atend_domiciliar ad on ad.localizador = f.co_unico_atend_prof "
             "inner join cnv_id_atendom cia on cia.sequenc = ad.sequenc "
             "where  aa.protocolo is not null")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 50
data = []
for tabela in resultados:
    print(f'{tabela.sequenc} - {tabela.codigo} - {tabela.id_cloud} ')

    response = requests.get(
        f'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentos/{tabela.id_cloud}',
        headers={'Authorization': f'Bearer {tokenEntidade}'})
    status = response.status_code
    print(response.content)
    print(status)
    dados = json.loads(response.content)
    procedimento_id = dados['classificacaoOrigemAtendimento']['id']
    if procedimento_id != 5100:
        data.append({
            "idIntegracao": f"{tabela.id_cloud}",
            "atendimento": {
                "idGerado": tabela.id_cloud,
                "classificacaoOrigemAtendimento": {
                    "id": 5100
                }
            }
        })
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
                stmt = text(f"UPDATE cnv_ajustaatendimentos SET protocolotipo = '{f_token_retorno}' WHERE id_cloud IN ({lista_sequenc_formatada})")
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
        stmt = text(f"UPDATE cnv_ajustaatendimentos SET protocolotipo = '{f_token_retorno}' WHERE id_cloud IN ({lista_sequenc_formatada})")
        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')