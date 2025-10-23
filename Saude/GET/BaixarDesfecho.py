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
urlPost = 'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDesfechos/'
#### Token da Endidade
tokenEntidade = '38096b6b-d3b5-4895-8c0b-92d355c7dee3'

query = text("select d.id_gerado as iddesfecho,c.sequenc,desfecho,id_profissional,id_cbo,id_especialidade,c.codigo,c.id_gerado "
             "from cnv_antenddesfecho d "
             "inner join cnv_id_atendom c on "
             "c.sequenc = d.sequenc  "
             "where desfecho = 4 and c.sequenc = 182004")

resultados = session.execute(query).fetchall()
# Lista para armazenar todos os dados

for tabela in resultados:
    print(f'{tabela.sequenc} - {tabela.codigo} - {tabela.id_gerado} - Desfecho {tabela.desfecho}')
    response = requests.get(f'https://saude.betha.cloud/api-saude-service-layer/v2/api/atendimentoDesfechos/{f_token_retorno}',
                            headers={'Authorization': f'Bearer {tokenEntidade}'})

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
            unidadeDestino = "Null"
            if item['unidadeDestino'] is not None:
                unidadeDestino = item['unidadeDestino']["id"]
            desfechoPersonalizado = "Null"
            if item['desfechoPersonalizado'] is not None:
                desfechoPersonalizado = item['desfechoPersonalizado']["id"]
            unidadeDestino = "Null"
            if item['classificacaoTipoTransporte'] is not None:
                clasTipoTransporte = item['classificacaoTipoTransporte']["id"]
            classificacaoSituacaoCliente = "Null"
            if item['classificacaoSituacaoCliente'] is not None:
                classificacaoSituacaoCliente = item['classificacaoSituacaoCliente']["id"]
            profissional = "Null"
            if item['profissional'] is not None:
                profissional = item['profissional']["id"]

            especialidade = "Null"
            if item['especialidade'] is not None:
                especialidade = item['especialidade']["id"]

            equipe = "Null"
            if item['equipe'] is not None:
                equipe = item['equipe']["id"]

            cbo = "Null"
            if item['cbo'] is not None:
                cbo = item['cbo']["id"]

            dataHoraObito = ""
            if item['dataHoraObito'] is not None:
                dataHoraObito = item['dataHoraObito']

            stmt = text(
                f"insert into cnv_profissionalcloud values ({unidadeDestino},{desfechoPersonalizado},{clasTipoTransporte},{classificacaoSituacaoCliente},{profissional},{especialidade},{equipe},);"
                f"{cbo},null,{dataHoraObito},{null},{idgerado})")
            session.execute(stmt)
            session.commit()
    else:
        print("Nenhum registro encontrado para listar.")

    print("-" * 50)
    print("Listagem concluída!")