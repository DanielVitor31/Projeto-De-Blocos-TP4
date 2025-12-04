import pandas as pd
from sqlalchemy import text
from conexao import conectar, desconectar


# Fiz o mais simples (tp4) possível para entregar logo devido a grandes chuvas e n perder o prazo, porém caso de tempo vou arrumar e melhorar

# No futuro tratar itens duplicados dentro do proprio json 

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
        print("Banco de dados está sendo criado/atualizado, aguarde...")

        with session.begin():
            session.execute(sql_upsert, registros)  

    except Exception as ex:
        print("Erro ao criar o banco de dados:", ex)
        erro = True

    finally:
        if session:
            desconectar(session)

    if not erro:
        print("Banco de dados inserido/atualizado com sucesso.")
        

    return





def deletar_clientes():
    df = pd.read_json('db/json_tp4.json')

    registros = df["clientes"].tolist()

    if not registros:
        print("Nenhum cliente no arquivo para deletar.")
        return

    tabela = "projeto_de_bloco_tp4.clientes"
    pk = "id_cliente"

    ids_clientes = [cli["id_cliente"] for cli in registros]

    sql_delete = text(f"""
        DELETE FROM {tabela}
        WHERE {pk} = :id_cliente
    """)

    erro = False
    session = None
    total_deletados = 0

    try:
        session = conectar()
        print("Removendo clientes do banco de dados, aguarde...")

        with session.begin():
            for id_cliente in ids_clientes:
                resultado = session.execute(sql_delete, {"id_cliente": id_cliente})
                if resultado.rowcount: # type: ignore
                    total_deletados += resultado.rowcount # type: ignore

    except Exception as ex:
        print("Erro ao deletar clientes do banco de dados:", ex)
        erro = True

    finally:
        if session:
            desconectar(session)

    if erro:
        return

    if total_deletados == 0:
        print("Nenhum desses clientes existe no banco de dados.")
    else:
        print(f"{total_deletados} cliente(s) removido(s) com sucesso.")

    return
