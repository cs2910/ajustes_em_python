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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDomiciliarProfissionais'
#### Token da Endidade

tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

'''Utilizando select direto'''

query = text("SELECT p.id_gerado as idproff,p.sequenc,p.id_unidade1 as id_unidade, "
             "p.id_profissional1 AS id_profissional,p.cbo1 AS id_cbo,p.equipe1 as id_equipe, "
             "p.idf1 as id_especialidade,p.desc1 as descpecialidade,a.codigo,a.id_gerado "
             "from cnv_atendprofissional p "
             "INNER join cnv_id_atendom a ON a.sequenc = p.sequenc "
             "inner join cnv_profissionalcloud c on	c.idGerado = p.id_gerado "
             "where c.profissional != p.id_profissional1")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 21
cids = []
data = []
for tabela in resultados:
    equipe = {}
    print(f'{tabela.sequenc} - {tabela.idproff} - {tabela.codigo} - {tabela.id_gerado}')
    if tabela.id_equipe is not None:
        equipe = {'equipe': {'id': tabela.id_equipe}}
    data.append({
        "idIntegracao": f"{tabela.sequenc}",
        "atendimentoDomiciliarProfissional": {
            "idGerado": tabela.idproff,
            "unidade":{"id": tabela.id_unidade},
            "profissional":{"id": tabela.id_profissional},
            "especialidade":{"id": tabela.id_especialidade},
            "cbo":{"id":tabela.id_cbo},
            **equipe
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
            stmt = text(f"UPDATE cnv_atendprofissional SET protocolo = '{f_token_retorno}',menssagemerro = null WHERE sequenc IN ({lista_sequenc_formatada})")
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
        stmt = text(
            f"UPDATE cnv_atendprofissional SET protocolo = '{f_token_retorno}',menssagemerro = null WHERE sequenc IN ({lista_sequenc_formatada})")
        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')