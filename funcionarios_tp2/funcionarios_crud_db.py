from conexao import conectar, desconectar
from sqlalchemy import text, func
from sqlalchemy.exc import NoResultFound
from db.funcionarios_tp2_comandos_sql import *
from models.models_tp2 import Funcionarios_models


def funcionarios_criar_resetar():
    """"
    Reseta e cria o banco de dados de funcionários do TP2.
    Os dados sugeridos estavam em execel e como precisava alterar nomes, e campos, optei por criar direto no SQL.
    Do contrario teria que fazer muita manipulação de dados com pandas e teria que criar outra planilha com outros dados.
    """
    erro = None
    try:
        session = conectar()
        print("Banco de dados esta sendo criado aguarde....")
        session.execute(sql_schema_criar_tp2)
        session.execute(sql_tabela_criar_funcionarios)
        session.execute(sql_table_inserir_funcionario)
        session.commit()
    except Exception as ex:
        print("Deu erro ao criar o banco de dados: ", ex)
        erro = True
    finally:
        desconectar(session)

    if (erro is None):
        print("Banco de dados criado com sucesso!")
    return


def funcionarios_exibir_db(retorno_tipo):
    try:
        session = conectar()
        print("Fazendo buscas no banco de dados aguarde....")
        funcionarios = session.query(Funcionarios_models).all()
        
        if not funcionarios:
          raise NoResultFound()
        
    except NoResultFound:
        print("Nenhum funcionário encontrado no departamento Técnico.")
        return
        
    except Exception as ex:
        print("Deu erro para exibir os funcionarios: ", ex)
        return
    finally:
        desconectar(session)

        return Funcionarios_models.dados_return(funcionarios, retorno_tipo)
    
    
    

def rodar_exercicio(consulta_fn, retorno_tipo, colunas=None, msg_sem_resultado="Nenhum registro encontrado."):
    """
    Antes fiz um desses para cada exercicio (deu umas 500 linhas), então criei uma forma de otimizar o código. (Deixei um exemplo na função acima "funcionarios_exibir_db")
    """
    
    try:
        session = conectar()
        print("Fazendo buscas no banco de dados aguarde....")

        funcionarios = consulta_fn(session)

        if not funcionarios:
            print(msg_sem_resultado)
            return

    except Exception as ex:
        print("Deu erro para exibir os funcionarios: ", ex)
        return

    finally:
        # garante que session sempre existe antes de tentar desconectar
        try:
            desconectar(session)
        except UnboundLocalError:
            pass

    return Funcionarios_models.dados_return(funcionarios, retorno_tipo, colunas)
    
def exercicio1_db(retorno_tipo):
    """
    Original:   Selecione todos os funcionários que trabalham no departamento de TI.
    Modificado: Selecione todos os funcionários que trabalham no departamento Técnico.
    """
    def consulta(session):
        return (
            session.query(Funcionarios_models)
            .filter(Funcionarios_models.departamento == "Técnico")
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=None,  # retorna o model inteiro
        msg_sem_resultado="Nenhum funcionário encontrado no departamento Técnico."
    )   
    
def exercicio2_db(retorno_tipo):
    """
    Original:   Selecione os nomes dos funcionários que possuem um salário maior que 5.000,00
    Modificado: Original
    """
    def consulta(session):
        return (
            session.query(
              Funcionarios_models.nome,
              Funcionarios_models.salario
          )
            .filter(Funcionarios_models.salario > 5000)
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["nome", "salario"],  
        msg_sem_resultado="Nenhum funcionário com salário maior que 5.000,00 foi encontrado."
    )   


def exercicio3_db(retorno_tipo):
    """
    Original:   Selecione o nome e a data de contratação dos funcionários que foram contratados após 01/01/2022.
    Modificado: Original
    """
    def consulta(session):
        return (
            session.query(
              Funcionarios_models.nome,
              Funcionarios_models.data_contratacao
           )
            .filter(Funcionarios_models.data_contratacao > "2022-01-01")
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["nome", "data_contratacao"],
        msg_sem_resultado="Nenhum funcionário contratado após 01/01/2022 foi encontrado."
    ) 



def exercicio4_db(retorno_tipo):
    """
    Original:   Selecione o departamento e o salário médio de cada departamento. 
    Modificado: Original
    """
    def consulta(session):
        return (
            session.query(
                Funcionarios_models.departamento,
                func.round(func.avg(Funcionarios_models.salario), 2).label("salario_medio")
            )
            .group_by(Funcionarios_models.departamento)
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["departamento", "salario_medio"],
        msg_sem_resultado="Nenhum departamento encontrado."
    )

def exercicio5_db(retorno_tipo):
    """
    Original:   Selecione o nome e o cargo dos funcionários que possuem "da Silva" no nome.
    Modificado: Original
    """
    def consulta(session):
        return (
            session.query(
              Funcionarios_models.nome,
              Funcionarios_models.cargo
           )
            .filter(Funcionarios_models.nome.like("%da Silva%"))
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["nome", "cargo"],
        msg_sem_resultado="Nenhum funcionário com 'da Silva' no nome foi encontrado."
    )

    
def exercicio6_db(retorno_tipo):
    """
    Original:   Selecione todos os funcionários que têm cargos de confiança.
    Modificado: Original
    """
    def consulta(session):
        return (
            session.query(
              Funcionarios_models.nome,
              Funcionarios_models.cargo_de_confianca
           )
            .filter(Funcionarios_models.cargo_de_confianca == True)
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["nome", "cargo_de_confianca"],
        msg_sem_resultado="Nenhum funcionário com cargos de confiança foi encontrado."
    )


def exercicio7_db(retorno_tipo):
    """
    Original:   Selecione o nome e o departamento dos analistas.
    Modificado: Selecione o nome e o departamento dos Montador Júnior.
    """
    def consulta(session):
        return (
            session.query(
              Funcionarios_models.nome,
              Funcionarios_models.departamento
           )
            .filter(Funcionarios_models.cargo == "Montador Júnior")
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["nome", "departamento"],
        msg_sem_resultado="Nenhum funcionário do departamento Montador Júnior foi encontrado."
    )
    
def exercicio8_db(retorno_tipo):
    """
    Original:   Selecione o nome dos funcionários e seus salários ordenados de forma decrescente pelo salário.
    Modificado: Original
    """
    def consulta(session):
        return (
            session.query(
              Funcionarios_models.nome,
              Funcionarios_models.salario
            
           )
            .order_by(Funcionarios_models.salario.desc())
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["nome", "salario"],
        msg_sem_resultado="Nenhum funcionário foi encontrado."
    )
    

def exercicio9_db(retorno_tipo):
    """
    Original:   Selecione o nome e o ID dos funcionários que foram contratados no ano de 2023.
    Modificado: Original
    """
    def consulta(session):
        # N achei via sqlalchemy como pegar só o ano, então fiz via sql para n perde tempo
        
        return (
            session.execute(
            text("""
                SELECT nome, id_funcionario FROM projeto_de_bloco_tp2.funcionarios
                WHERE EXTRACT(YEAR FROM data_contratacao) = 2023;
            """)
        ).fetchall()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["nome", "id_funcionario"],
        msg_sem_resultado="Nenhum funcionário foi encontrado."
    )



def exercicio10_db(retorno_tipo):
    """
    Original:   Selecione o nome dos funcionários que trabalham no departamento Jurídico e possuem um salário menor ou igual a 3.000,00.
    Modificado: Selecione o nome dos funcionários que trabalham no departamento Administração e possuem um salário menor ou igual a 3.000,00.
    """
    def consulta(session):
        # N achei via sqlalchemy como pegar só o ano, então fiz via sql para n perde tempo
        
        return (
            session.query(Funcionarios_models.nome)
            .filter(
                Funcionarios_models.departamento == "Administração",
                Funcionarios_models.salario <= 3000
            )
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["nome"],
        msg_sem_resultado="Nenhum funcionário foi encontrado."
    )


def exercicio11_db(retorno_tipo):
    """
    Original:   Selecione o nome dos funcionários que são Gerentes ou Diretores.
    Modificado: Selecione o nome dos funcionários que são Coordenador Administrativo ou Diretor.
    """
    def consulta(session):
        # N achei via sqlalchemy como pegar só o ano, então fiz via sql para n perde tempo
        
        return (
            session.query(Funcionarios_models.nome)
            .filter(Funcionarios_models.cargo.in_(["Coordenador Administrativo", "Diretor"]),)
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["nome"],
        msg_sem_resultado="Nenhum funcionário foi encontrado."
    )

def exercicio12_db(retorno_tipo):
    """
    Original:   Selecione o nome dos funcionários e os anos de experiência (considerando que estamos em 2025).
    Modificado: Original
    """
    def consulta(session):
        return (
            session.query(
                Funcionarios_models.nome,
                (2025 - func.year(Funcionarios_models.data_contratacao)).label("anos_experiencia")
            )
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["nome", "anos_experiencia"],
        msg_sem_resultado="Nenhum funcionário encontrado."
    )
    
def exercicio13_db(retorno_tipo):
    """
    Original:   Selecione o nome e o departamento dos funcionários, ordenados pelo nome em ordem alfabética.
    Modificado: Original
    """
    def consulta(session):
        return (
            session.query(
              Funcionarios_models.nome,
              Funcionarios_models.departamento
            )
            .order_by(Funcionarios_models.nome.asc())
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["nome", "departamento"],
        msg_sem_resultado="Nenhum funcionário encontrado."
    )


def exercicio14_db(retorno_tipo):
    """
    Original:   Selecione o nome e o cargo dos funcionários cujo nome começa com 'João'.
    Modificado: Original
    """
    def consulta(session):
        return (
            session.query(
              Funcionarios_models.nome,
              Funcionarios_models.cargo
           )
            .filter(Funcionarios_models.nome.like("João%"))
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["nome", "cargo"],
        msg_sem_resultado="Nenhum funcionário encontrado."
    )



def exercicio15_db(retorno_tipo):
    """
    Original:   Selecione a quantidade de funcionários em cada departamento.
    Modificado: Original
    """
    def consulta(session):
        return (
            session.query(
                Funcionarios_models.departamento,
                func.count(Funcionarios_models.id_funcionario).label("quantidade")
            )
            .group_by(Funcionarios_models.departamento)
            .all()
        )

    return rodar_exercicio(
        consulta_fn=consulta,
        retorno_tipo=retorno_tipo,
        colunas=["departamento", "quantidade"],
        msg_sem_resultado="Nenhum departamento encontrado."
    )
