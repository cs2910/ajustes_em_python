import  pyodbc
import requests, json

ip = '192.168.15.6'
port = '9003'
port1 = '9014'
database = 'Betha'
database1 = 'Tributos'

server = 'tribu'
server1 = 'canarana'

username = 'tecbth_stafba'
password = 'Xye8bTwgqi7bMo6zXhcyWwqYmyJsKn'



test = pyodbc.drivers()

strConn = f'Driver=Adaptive Server Anywhere 9.0;ENG={server};UID={username};PWD={password};DBN={database};LINKS=TCPIP(HOST={ip}:{port});'
conexao = pyodbc.connect(strConn)
cursor = conexao.cursor()

strConn1 = f'Driver=Adaptive Server Anywhere 9.0;ENG={server1};UID={username};PWD={password};DBN={database1};LINKS=TCPIP(HOST={ip}:{port1});'
conexao1 = pyodbc.connect(strConn1)
cursor1 = conexao1.cursor()

# Seleciona os dados do primeiro banco de dados
cursor.execute("SELECT * FROM bethadba.ruas where i_ruas > 2258")
dados = cursor.fetchall()
print(dados)
# Prepara a query de inserção com placeholders para os valores
colunas = [desc[0] for desc in cursor.description]
valores_placeholder = ", ".join(["?"] * len(colunas))
sql_insert = f"INSERT INTO bethadba.ruas ({', '.join(colunas)}) VALUES ({valores_placeholder})"
# Insere os dados no segundo banco de dados
for linha in dados:
    cursor1.execute(sql_insert, linha)

conexao1.commit()
