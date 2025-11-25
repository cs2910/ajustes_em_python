from sqlalchemy import create_engine
from sqlalchemy.engine import URL

## Conexao Saude
'''url = URL.create(
    drivername="postgresql",
    username="atibaia",
    host="faturamentoesus.atibaia.sp.gov.br",
    database="esus",
    password='3R|4XH4P.,D8',
    port='5432'
)'''

'''url = URL.create(
    drivername="postgresql",
    username="postgres",
    host="aws-svc-arrec01.betha.com.br",
    database="saude3",
    ##database="ajustafolha",
    ##database="ajustaeduc",
    password='postgres',
    port='5432'
)'''


## Conexao Ajustes
url = URL.create(
    drivername="postgresql",
    username="postgres",
    host="localhost",
    database="ajustes",
    ##database="ajustafolha",
    ##database="ajustaeduc",
    password='Cp@142536',
    port='5432'
)

## Conexao Livro
'''url = URL.create(
    drivername="postgresql",
    username="postgres",
    host="localhost",
    database="livroatibaia",
    password='Cp@142536',
    port='5432'
)'''

## Conexao Patrimonio
'''url = URL.create(
    drivername="postgresql",
    username="postgres",
    host="localhost",
    database="protocolo",
    password='Cp@142536',
    port='5432'
)

url = URL.create(
    drivername="postgresql",
    username="postgres",
    host="localhost",
    database="dourados",
    password='Cp@142536',
    port='5432'
)


url = URL.create(
    drivername="postgresql",
    username="postgres",
    host="localhost",
    database="murtinho",
    password='Cp@142536',
    port='5432'
)'''


engine = create_engine(url)
connection = engine.connect()

