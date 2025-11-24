import pandas as pd
from sqlalchemy import text
from conexao import conectar, desconectar


def exibirdados():
    df = pd.read_json('db/json_tp4.json')
    print(df)
    
    return 