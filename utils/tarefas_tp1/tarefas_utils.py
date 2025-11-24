from tabulate import tabulate
from constantes.constantes_tp1 import COMPRAS_CABECALHO, COMPRAS_CABECALHO_REDUZIDO

def tabulete_person(compras, reduzida, tipo):
    """
    Gostei do tabulete é algo bem simples de usar para exibir tabelas.
    Poderia usar ele direto, porém devido a opção de reduzido ou completo, optei por criar essa função para facilitar.
    """
    
    cabecalho = COMPRAS_CABECALHO_REDUZIDO if reduzida else COMPRAS_CABECALHO
    compras = compras if isinstance(compras[0], list)  else [compras] # Apenas pq o tabulete precisa receber uma lista de lista ]

    print(tabulate(compras, headers=cabecalho, tablefmt=tipo))
    return 