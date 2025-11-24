from sqlalchemy import Column, Integer, String, Float, Boolean, Date
from sqlalchemy.ext.declarative import declarative_base
from constantes.constantes_tp2 import FUNCIONARIOS_CABECALHO

Base = declarative_base()

class Funcionarios_models(Base):
    __tablename__ = "funcionarios"
    __table_args__ = {"schema": "projeto_de_bloco_tp2"}

    id_funcionario = Column(Integer, primary_key=True, autoincrement=True)
    nome = Column(String(40), nullable=False)
    cargo = Column(String(30), nullable=False)
    departamento = Column(String(30), nullable=False)
    salario = Column(Float, nullable=False)
    data_contratacao = Column(Date, nullable=False)
    cargo_de_confianca = Column(Boolean, nullable=False)

    # campos padrão 
    FUNCIONARIOS_COLUNAS = FUNCIONARIOS_CABECALHO

    # ----- MÉTODOS DE CONVERSÃO -----

    @classmethod
    def para_lista(cls, funcionarios, campos=None):
        """
        Retorna lista de listas.
        Se 'campos' não for passado, usa todos os campos padrão.
        """
        if campos is None:
            campos = cls.FUNCIONARIOS_COLUNAS

        return [
            [getattr(f, campo) for campo in campos]
            for f in funcionarios
        ]

    @classmethod
    def para_dict(cls, funcionarios, campos=None):
        if campos is None:
            campos = cls.FUNCIONARIOS_COLUNAS

        resultado = {}

        for f in funcionarios:
            item = {campo: getattr(f, campo) for campo in campos}
            # mantém id_funcionario como chave principal
            resultado[f.id_funcionario] = item

        return resultado

    @classmethod
    def dados_return(cls, funcionarios, retorno_tipo, campos=None):
        if retorno_tipo == "lista":
            return cls.para_lista(funcionarios, campos=campos)
        else:
            return cls.para_dict(funcionarios, campos=campos)
