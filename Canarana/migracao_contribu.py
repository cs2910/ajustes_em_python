import psycopg2
import pyodbc
ip = '192.168.15.8'
port = '9014'
database = 'Tributos'
server = 'final'
username = 'tecbth_stafba'
password = 'Xye8bTwgqi7bMo6zXhcyWwqYmyJsKn'

# Conexão com o banco de dados PostgreSQL


conexao_postgres = psycopg2.connect(
    host="localhost",
    port="5432",
    database="migracao",
    user="postgres",
    password="Cp@142536"
)
cursor_postgres = conexao_postgres.cursor()
tabela = 'contrib'

# Conexão com o banco de dados Sybase
strConn = f'Driver=Adaptive Server Anywhere 9.0;ENG={server};UID={username};PWD={password};DBN={database};LINKS=TCPIP(HOST={ip}:{port});'
conexao = pyodbc.connect(strConn)
cursor_sybase = conexao.cursor()

# Seleciona os dados da tabela no PostgreSQL

cursor_postgres.execute(f"select c.contribuinte_id, p.pessoa_id,p.pessoa_razao_social, p.pessoa_cpf_cnpj, p.pessoa_id_conv_trib ,"
                        f"replace(replace(l.logradouro_cep,'.',''),'-','') as ceps, l.logradouro_endereco,b.bairro_nome, cid.cidade_nome, e.estado_sigla "
                        f"from central.pessoa p "
                        f"inner join tributario.contribuinte c on"
                        f"	c.contribuinte_pessoa_id = p.pessoa_id "
                        f"left outer join central.logradouro l on"
                        f"	l.logradouro_id = p.pessoa_logradouro_id "
                        f"left outer join central.bairro b on"
                        f"	b.bairro_id = l.bairro_codigo "
                        f"left outer join central.cidade cid on"
                        f"	cid.cidade_id = b.cidade_id"
                        f" left outer join central.estado e on"
                        f"	e.estado_id = cid.estado_id	where c.contribuinte_id is not null order by p.pessoa_id")



dados = cursor_postgres.fetchall()
cursor_sybase.execute(f"CREATE TABLE cnvf_{tabela} ("
                      f"cont_id integer, "
                      f"pessoa_id integer,"
                      f"razao_social varchar(500),"
                      f"cpf_cnpj varchar(14),"
                      f"conversao_id integer,"
                      f"ceps varchar(10), logra varchar(150), bair varchar(100), cid varchar(100), uf varchar(2))")

# Preparar a query de inserção no Sybase
sql_insert = f"INSERT INTO cnvf_{tabela}(cont_id,pessoa_id,razao_social,cpf_cnpj,conversao_id, ceps, logra , bair, cid , uf) VALUES (?, ?, ?, ?, ?, ? , ?, ?, ?, ?)"

# Inserir os dados no Sybase
for linha in dados:
    cursor_sybase.execute(sql_insert, linha)

# Confirmar as mudanças no Sybase
conexao.commit()

# Fechar os cursores e as conexões
cursor_postgres.close()
conexao_postgres.close()
cursor_sybase.close()
conexao.close()
