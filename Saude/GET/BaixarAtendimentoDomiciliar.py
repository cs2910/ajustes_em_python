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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentosDomiciliares/'
#### Token da Endidade
tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

# Lista para armazenar todos os dados
all_cbo_data = []

processed_ids = set()
# Parâmetros de paginação
offset = 0
limit = 500  # Limite de registros por requisição
has_next = True

# Loop para buscar todos os dados
while has_next:
    print(f"Buscando dados com offset: {offset}...")
    params = {'offset': offset, 'limit': limit}
    response = requests.get(
        urlPost,
        headers={'Authorization': f'Bearer {tokenEntidade}'},
        params=params
    )

    if response.status_code == 200:
        data = json.loads(response.content)
        content_list = data.get('content', [])
        all_cbo_data.extend(content_list)
        print(content_list)
        has_next = data.get('hasNext', False)
        if has_next:
            offset += limit
    else:
        print(f"Erro na requisição: {response.status_code}")
        break

    print("-" * 50)
    print(f"Total de registros encontrados: {len(all_cbo_data)}")
    print("-" * 50)

    if all_cbo_data:
        for item in all_cbo_data:
            idGerado = item['idGerado']

            # Verifica se o ID já foi processado
            if idGerado in processed_ids:
                print(f"Registro com idGerado {idGerado} já existe. Pulando.")
                continue  # Pula para o próximo item do loop

            # Se o ID não foi processado, adiciona ao conjunto
            processed_ids.add(idGerado)
            cidPrincipal = "Null"
            cidsecundario1 = "Null"
            cidsecundario2 = "Null"
            classificacaoDestino = "Null"
            classificacaoFrequencia = "Null"
            classificacaoModalidade = "Null"
            classificacaoTipoCuidador = "Null"
            unidadePrestadora = "Null"
            id_primeiro_profissional = "Null"
            classificacaoProcedencia = "Null"
            classificacaoSituacao = "Null"
            cliente = "Null"
            codigo = "Null"
            dataavaliacao = ""
            profissionalSolicitante = "Null"
            dataUltimoatendimento = ""
            dataSolicitacao = ""
            ultimoDesfecho = "Null"
            unidadeSolicitante = "Null"
            codigo = int(item['codigo'])
            if codigo < 215:
                continue
            if item['atendimentoDomiciliarAvaliarElegibilidade'] is not None:
                if item['atendimentoDomiciliarAvaliarElegibilidade']['cidPrincipal'] is not None:
                    cidPrincipal = item['atendimentoDomiciliarAvaliarElegibilidade']['cidPrincipal']['id']
                if item['atendimentoDomiciliarAvaliarElegibilidade']['cidSecundario1'] is not None:
                    cidsecundario1 = item['atendimentoDomiciliarAvaliarElegibilidade']['cidSecundario1']['id']
                if item['atendimentoDomiciliarAvaliarElegibilidade']['cidSecundario2'] is not None:
                    cidsecundario2 = item['atendimentoDomiciliarAvaliarElegibilidade']['cidSecundario2']['id']
                if item['atendimentoDomiciliarAvaliarElegibilidade']['classificacaoDestino'] is not None:
                    classificacaoDestino = item['atendimentoDomiciliarAvaliarElegibilidade']['classificacaoDestino']['id']
                if item['atendimentoDomiciliarAvaliarElegibilidade']['classificacaoFrequencia'] is not None:
                    classificacaoFrequencia = item['atendimentoDomiciliarAvaliarElegibilidade']['classificacaoFrequencia']['id']
                if item['atendimentoDomiciliarAvaliarElegibilidade']['classificacaoModalidade'] is not None:
                    classificacaoModalidade = item['atendimentoDomiciliarAvaliarElegibilidade']['classificacaoModalidade']['id']
                if item['atendimentoDomiciliarAvaliarElegibilidade']['classificacaoTipoCuidador'] is not None:
                    classificacaoTipoCuidador = item['atendimentoDomiciliarAvaliarElegibilidade']['classificacaoTipoCuidador']['id']
                if item['atendimentoDomiciliarAvaliarElegibilidade']['unidadePrestadora'] is not None:
                    unidadePrestadora = item['atendimentoDomiciliarAvaliarElegibilidade']['unidadePrestadora']['id']
            profissionais = item['atendimentoDomiciliarProfissionais']
            if profissionais:
                id_primeiro_profissional = profissionais[0]['id']
            if item['classificacaoProcedencia'] is not None:
                classificacaoProcedencia = item['classificacaoProcedencia']['id']

            if item['classificacaoSituacao'] is not None:
                classificacaoSituacao = item['classificacaoSituacao']['id']
            if item['cliente'] is not None:
                cliente = item['cliente']['id']
            codigo = item['codigo']
            if item['dataAvaliacao'] is not None:
                dataavaliacao = item['dataAvaliacao']
            if item['dataUltimoAtendimento'] is not None:
                dataUltimoatendimento = item['dataUltimoAtendimento']
            if item['profissionalSolicitante'] is not None:
                profissionalSolicitante = item['profissionalSolicitante']['id']
            if item['ultimoDesfecho'] is not None:
                ultimoDesfecho = item['ultimoDesfecho']['id']
            if item['unidadeSolicitante'] is not None:
                unidadeSolicitante = item['unidadeSolicitante']['id']
            if item['dataSolicitacao'] is not None:
                dataSolicitacao = item['dataSolicitacao']

            stmt = text(
                f"insert into cnv_atenddomiccloud values ("
                f"{cidPrincipal},{cidsecundario1},{cidsecundario2},{classificacaoDestino},{classificacaoFrequencia},{classificacaoModalidade},{classificacaoTipoCuidador},{unidadePrestadora},"
                f"{id_primeiro_profissional},{classificacaoProcedencia},{classificacaoSituacao},{cliente},{codigo},'{dataavaliacao}','{dataSolicitacao}','{dataUltimoatendimento}',"
                f"{idGerado},{profissionalSolicitante},{ultimoDesfecho},{unidadeSolicitante})")
            ##print(stmt)
            session.execute(stmt)
            session.commit()
    else:
        print("Nenhum registro encontrado para listar.")

    print("-" * 50)
    print("Listagem concluída!")