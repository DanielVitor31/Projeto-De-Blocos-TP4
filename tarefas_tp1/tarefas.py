from tabulate import tabulate
from .tarefas_crud_db import compras_carregar, compra_carregar, compras_adicionar_db, compras_exibir_status_db, compras_status_atualizar_db
from db.tarefas_tp1_lista import compras_db
from utils.tarefas_tp1.tarefas_utils import tabulete_person
from textwrap import dedent
from constantes.constantes_tp1 import COMPRAS_CABECALHO, COMPRAS_CABECALHO_REDUZIDO

"""
Tentei seguir um pouco as boas praticas do LP de separar a parte de interface da parte de "banco de dados" 
O enunciado pede "tarefas", porém optei por "compras" para ficar mais condizente com o tema do projeto (Loja de montagem de PC).
"""


def compras_exibir(reduzida):
    """Exibe todas as compras cadastradas no banco de dados."""

    compras_atualizdas = compras_carregar(compras_db, reduzida)
    tabulete_person(compras_atualizdas, reduzida, "github")
    return


def compra_exibir(reduzida):
    """Exibe uma compra específica cadastrada no banco de dados."""
    
    compras_todas = compras_carregar(compras_db, reduzida) #Pq precisei chamar a função "compras_carregar" invés de pegar a lista original completa é pq a função "compras_carregar" faz o tratamento de reduzir ou n a lista.
    compra_atualizada = compra_carregar(compras_todas, False)
    
    if (compra_atualizada is None):
        print("Compra não encontrada.")
        return

    tabulete_person(compra_atualizada, reduzida, "github")
    return


def compras_adicionar(reduzida):
    """Adiciona uma nova compra no banco de dados. Obs: Ainda n fiz validação de dados."""
    
    id_ultimo = int(compras_db[-1][0]) # Pega o último ID da lista de compras para incrementar na nova compra

    nome_comprador = input("Informe o nome do comprador: ")
    placa_mae = input("Informe a placa mãe: ")
    fonte = input("Informe a fonte: ")
    armazenamento_1 = input("Informe o armazenamento 1: ")
    armazenamento_2 = input("Informe o armazenamento 2 (ou deixe vazio): ")
    processador = input("Informe o processador: ")
    memoria_ram = input("Informe a memória RAM: ")
    placa_video = input("Informe a placa de vídeo: ")
    data_compra = input("Informe a data da compra (YYYY-MM-DD HH:MM): ")
    total = float(input("Informe o total da compra: "))
    status = input("Informe o status da compra (True para concluída, False para pendente): ").lower()


    nova_compra = [
        id_ultimo + 1, nome_comprador, placa_mae, fonte,
        armazenamento_1, armazenamento_2, processador,
        memoria_ram, placa_video, data_compra, total, status
    ]

    verificar_adicao = compras_adicionar_db(compras_db, nova_compra)
    
    if (verificar_adicao is None):
        print("Erro ao adicionar novo item. Tente novamente.")
    else:
        print("Novo item adicionado com sucesso.\n")
        print("Lista atualizada :")
        compras_exibir(reduzida)

    return 



def compras_status_atualizar(reduzida):
    """
    Exibe as compras por status e permite atualizar o status de uma compra.
    Tem tratamento para caso n tenha nenhuma compra com o status escolhido ou que o status já seja o mesmo.
    """
    
    while (True):
        escolha = input(dedent("""
        Escolha "True" para ver as tarefas concluídas e "False" para ver as tarefas pendentes.
        ========= Status =========
        True
        False
        ==========================
        """)).lower()

        if escolha not in ("true", "false"):
            continue

        compras_status = compras_exibir_status_db(compras_db, reduzida, escolha)


        if (len(compras_status) == 0):
            print(f"Nenhuma tarefa está como {escolha}")
            return
        
        # O exercicio pede para enumerar as tarefas, então optei por usar o tabulate direto e n o personalizado que fiz.
        cabecalho = COMPRAS_CABECALHO_REDUZIDO if reduzida else COMPRAS_CABECALHO
        cabecalho.insert(0, "Nº")
        print(tabulate(compras_status, headers=cabecalho, showindex=range(1, len(compras_status) + 1), tablefmt="github"))
        print()

        atualizar_tarefa = input(f"Deseja atualizar o status de alguma compra de {escolha} para {not escolha}? (s/n): ").lower()
        
        if atualizar_tarefa != "s":
            print("Sua escolha foi diferente de 's'. Nada será atualizado.")
            return

        compras_status_indice = compra_carregar(compras_db, True)
        if (compras_status_indice is None):
            print("Compra não encontrada.")
            return
        
        print("Escolha foi", compras_status_indice, "sua lista foi atualizada")
        compras_status_atualizar_db(compras_db, compras_status_indice, not escolha)
        
        
        return
