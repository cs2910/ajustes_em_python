from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey,JSON, TEXT
from sqlalchemy.orm import declarative_base
from conexao import engine


Base = declarative_base()


class Baixapatri(Base):
    __tablename__ = 'baixapatri'
    id = Column(Integer(), primary_key=True)
    idbaixa = Column(Integer)
    databaixa = Column(DateTime)
    placa = Column(Integer)
    protocolo = Column(String(100))
    idcloud = Column(Integer)

def init_db():
    Base.metadata.create_all(bind=engine)


if __name__ == '__main__':
    init_db()