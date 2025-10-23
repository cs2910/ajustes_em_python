from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from conexao import engine
import requests, json

# Conexão com banco de dados
Session = sessionmaker(bind=engine)
session = Session()

# URL do serviço
urlPost = 'https://pessoal.betha.cloud/service-layer/v1/api/configuracao-evento/'
# Token da Entidade
tokenEntidade = '5f8a32ac-701e-4282-a342-c16781f99713'

# Consulta ao banco
query = text("SELECT * FROM eventoadd WHERE protocolo IS NULL")
##query = text("select * from procpescep where novocep is not null and id in ('13047354','13048186','13048404','13048538')")
resultados = session.execute(query).fetchall()

# Armazenar dados em lotes
batch_size = 50
data_batch = []

for tabela in resultados:
    data_batch.append({
        "idIntegracao": f"{tabela.id}",
        "pessoasEnderecos": {
            "idGerado": {"id": tabela.id},
            "cep": tabela.novocep,
        }
    })

    # Enviar quando atingir o tamanho do lote
    if len(data_batch) == batch_size:
        dados = json.dumps(data_batch)
        response = requests.patch(urlPost, headers={'Authorization': f'Bearer {tokenEntidade}',
                                                    'content-type': 'application/json'}, data=dados)
        status = response.status_code

        print(response.content)
        print(status)

        if status == 200:
            recurrence = json.loads(response.content)
            f_token_retorno = recurrence.get('idLote', '')
            print(f'Protocolo {f_token_retorno}')

            for item in data_batch:
                stmt = text(
                    f"UPDATE procpescep SET protocolo = '{f_token_retorno}' WHERE id = '{item['idIntegracao']}'")
                session.execute(stmt)

            session.commit()
        else:
            print('Erro na execução')

        # Limpa a lista para o próximo lote
        data_batch = []

# Enviar qualquer restante que não tenha completado um lote de 50
if data_batch:
    dados = json.dumps(data_batch)
    response = requests.patch(urlPost,
                              headers={'Authorization': f'Bearer {tokenEntidade}', 'content-type': 'application/json'},
                              data=dados)
    status = response.status_code

    print(response.content)
    print(status)

    if status == 200:
        recurrence = json.loads(response.content)
        f_token_retorno = recurrence.get('idLote', '')
        print(f'Protocolo {f_token_retorno}')

        for item in data_batch:
            stmt = text(f"UPDATE procpescep SET protocolo = '{f_token_retorno}' WHERE id = '{item['idIntegracao']}'")
            session.execute(stmt)

        session.commit()
    else:
        print('Erro na execução')