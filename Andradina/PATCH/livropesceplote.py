from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from conexao import engine
import requests, json

# Conexão com banco de dados
Session = sessionmaker(bind=engine)
session = Session()

# URL do serviço
urlPost = 'https://e-gov.betha.com.br/glb/service-layer/v2/api/pessoas/'
# Token da Entidade
tokenEntidade = 'ebe84f54-89ee-479f-a5f7-b9058ffe5ad6'

# Consulta ao banco
##query = text("select * from pessoalivro where novocep is not null and protocolo is null and id in ('12145','28177621')")
query = text("select * from pessoalivro where novocep is not null and protocolo is null")
resultados = session.execute(query).fetchall()

# Armazenar dados em lotes
batch_size = 50
data_batch = []

for tabela in resultados:
    data_batch.append({
        "idIntegracao": f"{tabela.id}",
        "pessoa": {
            "idGerado": {
                "iPessoas": int(tabela.id)
            },
            "enderecos": [
                {
                    "idGerado": {
                        "tipoEndereco": tabela.tipo,
                        "iPessoas": int(tabela.id)
                    },
                    "op": "ATUALIZAR",
                    "cep": tabela.novocep
                }
            ]
        }
    })


   ## print(data_batch)
    # Enviar quando atingir o tamanho do lote
    if len(data_batch) == batch_size:
##        dados = json.dumps(data_batch)
        dados = json.dumps(data_batch, ensure_ascii=False)

    ##    print(dados)
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
                stmt = text("UPDATE pessoalivro SET protocolo = :protocolo WHERE id = :id AND tipo = :tipo")
                session.execute(stmt,{"protocolo": f_token_retorno, "id": item["idIntegracao"], "tipo": item["pessoa"]["enderecos"][0]["idGerado"]["tipoEndereco"]})

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
            stmt = text("UPDATE pessoalivro SET protocolo = :protocolo WHERE id = :id AND tipo = :tipo")
            session.execute(stmt, {"protocolo": f_token_retorno, "id": item["idIntegracao"],
                                   "tipo": item["pessoa"]["enderecos"][0]["idGerado"]["tipoEndereco"]})
        session.commit()
    else:
        print('Erro na execução')