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

query = text("SELECT p.sequenc,"
             "CASE WHEN p.id_unidade2 IS NOT NULL THEN p.id_unidade2 ELSE p.id_unidade1 END AS id_unidade, "
             "CASE WHEN p.id_profissional2 IS NOT NULL THEN p.id_profissional2 ELSE p.id_profissional1 END AS id_profissional,"
             "CASE WHEN p.cbo2 IS NOT NULL THEN p.cbo2 ELSE p.cbo1 END AS id_cbo,"
             "CASE WHEN p.equipe2 IS NOT NULL THEN p.equipe2 ELSE p.equipe1 END as id_equipe,"
             "CASE WHEN p.idf2 IS NOT NULL THEN p.idf2 ELSE p.idf1 END as id_especialidade,"
             "CASE WHEN p.desc2 IS NOT NULL THEN p.desc2 ELSE p.desc1 END as descpecialidade,"
             "a.codigo,a.id_gerado "
             "from cnv_atendprofissional p "
             "INNER join cnv_id_atendom a ON a.sequenc = p.sequenc "
             "where a.id_gerado is not null  and "
             "p.protocolo IS not null and p.sequenc = 530505")

##query = text("SELECT * FROM ajustedtisencao where protocolo is null")
resultados = session.execute(query).fetchall()
batch_size = 1
cids = []
data = []
for tabela in resultados:
    equipe = {}
    print(f'{tabela.sequenc} - {tabela.codigo} - {tabela.id_gerado}')
    if tabela.id_equipe is not None:
        equipe = {'equipe': {'id': tabela.id_equipe}}
    data.append({
        "idIntegracao": f"{tabela.sequenc}",
        "atendimentoDomiciliarProfissional": {
            "atendimentoDomiciliar":{
                "id":  tabela.id_gerado
            },
            "unidade":{"id": tabela.id_unidade},
            "profissional":{"id": tabela.id_profissional},
            "especialidade":{"id": tabela.id_especialidade},
            "cbo":{"id":tabela.id_cbo},
            **equipe,
            "principal": 'false',
            "excluido": 'false'
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
            f"UPDATE cnv_atendprofissional SET protocolo = '{f_token_retorno}',menssagemerro = null WHERE sequenc IN ({lista_sequenc_formatada})")
        session.execute(stmt)
        session.commit()
    else:
        print(f'erro execução')