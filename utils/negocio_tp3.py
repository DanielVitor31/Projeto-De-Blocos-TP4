def converter_dicionario(dados, limite):
    limite_contador = 0

    for item in dados:

        primeira_chave = next(iter(item.keys()))  # pega a primeira chave
        chave_id = item[primeira_chave]

        item_sem_id = {}
        for chave, valor in item.items():
            if chave != primeira_chave:
                item_sem_id[chave] = valor

        print(f"ID: {chave_id}")
        for chave, valor in item_sem_id.items():
            print(f"  {chave}: {valor}")
        print()

        if limite and limite_contador == 5:
            break
        limite_contador += 1

    return


def converter_lista(dados, tipo_retorno, limite):
    limite_contador = 0

    for item in dados:

        lista_valores = []
        for chave, valor in item.items():
            lista_valores.append(valor)

        print(lista_valores)

        if limite and limite_contador == 5:
            break
        limite_contador += 1
