def compras_carregar(compras, reduzida):
    """Carrega as compras do banco de dados."""
    
    if (not reduzida):
        return compras

    compras_reduzidas = []
    for compra in compras:
        nova_compra = compra[:3] + compra[8:]
        compras_reduzidas.append(nova_compra)

    return compras_reduzidas


def compra_carregar(compras, indice):
    """
    Carrega uma compra específica do banco de dados.
    Se "indice" for True, retorna o índice da compra na lista.
    Se "indice" for False, retorna a compra em si.
    """
    
    # Não vou tratar caso seja string pq no futuro vou usar UUID
    id_escolhido = int(input("Informe o id que deseja escolher: "))

    for i, compra in enumerate(compras):
        if (compra[0] == id_escolhido):
            if (indice):
                return i
            else:
                return compra
        else:
            continue
    return None


def compras_adicionar_db(compras, nova_compra):
    """Apenas Adciona a nova compra na lista de compras."""
    
    compras.append(nova_compra)
    print(compras)
    return True


def compras_exibir_status_db(compras, reduzida, escolha):
    """Exibe as compras com base no status escolhido."""

    escolha_bool = escolha == "true" # Converte a escolha para booleano
    compras_status = []

    for compra in compras:

        if (compra[-1] == escolha_bool): # Peguei o -1 pq o status é o último item da lista
            if (reduzida):
                compras_status.append(compra[:3] + compra[8:])
            else:
                compras_status.append(compra)

    return compras_status


def compras_status_atualizar_db(compras, indice, escolha):
    """ Atualiza o status da compra no banco de dados."""
    
    compra = compras[indice]

    escolha_bolean = escolha == "true"

    if (compra[-1] == escolha_bolean):
        print(
            f"Você escolheu {escolha}, mas essa compra já é {escolha}. Nada mudou")
    else:
        compra[-1] = escolha_bolean
        print(f"Sua compra foi atualizada com sucesso para {escolha}")
