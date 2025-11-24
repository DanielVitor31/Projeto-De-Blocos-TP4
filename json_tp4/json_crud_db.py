import pandas as pd
from sqlalchemy import text
from conexao import conectar, desconectar


def inserir_atualizar():
    df = pd.read_json('db/json_tp4.json')

    
    registros = df["clientes"].tolist()  

    if not registros:
        print("Nenhum registro para inserir.")
        return

    tabela = "projeto_de_bloco_tp4.clientes"
    pk = "id_cliente"

    colunas = list(registros[0].keys())  

    cols_str = ", ".join(colunas)
    vals_str = ", ".join([f":{col}" for col in colunas])
    set_str = ", ".join(
        [f"{col} = EXCLUDED.{col}" for col in colunas if col != pk]
    )

    sql_upsert = text(f"""
        INSERT INTO {tabela} ({cols_str}) 
        VALUES ({vals_str}) 
        ON CONFLICT ({pk}) 
        DO UPDATE SET {set_str}
    """)

    erro = False
    session = None

    try:
        session = conectar()
        print("Banco de dados está sendo criado, aguarde...")

        with session.begin():
            session.execute(sql_upsert, registros)  

    except Exception as ex:
        print("Erro ao criar o banco de dados:", ex)
        erro = True

    finally:
        if session:
            desconectar(session)

    if not erro:
        print("Banco de dados criado com sucesso!")

    return
