from utils.menu_utils import menu_validar_entrada
from constantes.constantes import MENU_SAIR
from constantes.constantes_tp1 import COMPRAS_REDUZIDO, MENU_OPCOES_NUM_TP1
from tarefas_tp1.tarefas import compras_exibir, compra_exibir, compras_adicionar, compras_status_atualizar
from textwrap import dedent


def menu_interface():
    """Exibe a interface do menu principal."""

    print(dedent("""
        ========= MENU TP1 =========
        [1] Exibir Compras
        [2] Exibir Compra
        [3] Adicionar Compra
        [4] Atualizar Status da Compra
        [0] Voltar para o Menu Principal
        ============================
    """))


def menu_opcao_validar():
    """Valida a opção escolhida no menu principal."""

    while (True):
        menu_interface()
        opcao_escolhida = input("Escolha uma opção do menu: ")
        print()

        opcao_verificada = menu_validar_entrada(opcao_escolhida, MENU_OPCOES_NUM_TP1)

        if (opcao_verificada is None):
            print(f'Você escolheu uma opção "{opcao_escolhida}" não valido')
            print()

        return opcao_verificada


def menu_exibir_tp1():
    """
    Exibe o menu principal e encaminha para sua função.
    Oq seria "COMPRAS_REDUZIDO" como usei o tabulete para exibir as tabelas, com muitos dados e colunas fica uma "zona", então optei por deixar uma versão reduzida como padrão.
    Caso queira ver todos dados basta alterar a constante "COMPRAS_REDUZIDO" para "False".
    """

    while (True):
        opcao_validade = menu_opcao_validar()
        # N entendi muito bem pq o "MENU_SAIR" n funciona no match direito, então fiz separado
        if (opcao_validade == MENU_SAIR):
            print("Você voltou para o Menu Principal.")
            return

        # Não vou criar um "default" pq a própria função "menu_opcao_validar" já faz esse tratamento.
        match (opcao_validade):
            case 1:
                compras_exibir(COMPRAS_REDUZIDO)
            case 2:
                compra_exibir(COMPRAS_REDUZIDO)
            case 3:
                compras_adicionar(COMPRAS_REDUZIDO)
            case 4:
                compras_status_atualizar(COMPRAS_REDUZIDO)
