import pandas as pd
import os

import auxiliar  # assumindo que você já tem isso implementado


# Mapeia quais metas aplicar por tipo de ramo
metas_por_ramo = {
    "Justiça Estadual": [
        "meta1",
        "meta2a",
        "meta2b",
        "meta2c",
        "meta2ant",
        "meta4a",
        "meta4b",
        "meta6",
        "meta7a",
        "meta7b",
        "meta8a",
        "meta8b",
        "meta10a",
        "meta10b"
    ],
    "Justiça do Trabalho": [
        "meta1",
        "meta2a",
        "meta2ant",
        "meta4a",
        "meta4b"
    ],
    "Justiça Federal": [
        "meta1",
        "meta2a",
        "meta2b",
        "meta2ant",
        "meta4a",
        "meta4b",
        "meta6",
        "meta7a",
        "meta7b",
        "meta8a",
        "meta8b",
        "meta10a"
    ],
    "Justiça Militar da União": [
        "meta1",
        "meta2a",
        "meta2b",
        "meta2ant",
        "meta4a",
        "meta4b"
    ],
    "Justiça Militar Estadual": [
        "meta1",
        "meta2a",
        "meta2b",
        "meta2ant",
        "meta4a",
        "meta4b"
    ],
    "Justiça Eleitoral": [
        "meta1",
        "meta2a",
        "meta2b",
        "meta2ant",
        "meta4a",
        "meta4b"
    ],
    "TST": [
        "meta1",
        "meta2a",
        "meta2b",
        "meta2ant",
        "meta4a",
        "meta4b"
    ],
    "STJ": [
        "meta1",
        "meta2ant",
        "meta4a",
        "meta4b",
        "meta6",
        "meta7a",
        "meta7b",
        "meta8a", # no arquivo tá só 8
        "meta10a" # no arquivo tá só 10
    ]
}

# Todas as funções disponíveis no auxiliar
funcoes_metas = {
    "meta1": auxiliar.meta1,
    "meta2a": auxiliar.meta2A,
    "meta2b": auxiliar.meta2B,
    "meta2c": auxiliar.meta2C,
    "meta2ant": auxiliar.meta2ANT,
    "meta4a": auxiliar.meta4A,
    "meta4b": auxiliar.meta4B,
    "meta6": auxiliar.meta6,
    "meta7a": auxiliar.meta7A,
    "meta7b": auxiliar.meta7B,
    "meta8a": auxiliar.meta8A,
    "meta8b": auxiliar.meta8B,
    "meta10a": auxiliar.meta10A,
    "meta10b": auxiliar.meta10B,
}

# Caminho para a pasta com os CSVs
pasta = "./Dados"

# Para cada arquivo na pasta
for nome_arquivo in os.listdir(pasta):
    if nome_arquivo.endswith('.csv'):
        caminho_arquivo = os.path.join(pasta, nome_arquivo)
        
        try:
            # Carrega o CSV
            df = pd.read_csv(caminho_arquivo)

            # Verifica se as colunas necessárias existem
            if 'ramo_justica' not in df.columns or 'sigla_tribunal' not in df.columns:
                print(f"{nome_arquivo}: coluna 'ramo_justica' ou 'sigla_tribunal' não encontrada.")
                continue

            # Aplica a regra de substituição
            df['ramo_tratado'] = df.apply(
                lambda row: row['sigla_tribunal'] if row['ramo_justica'] == 'Tribunais Superiores' else row['ramo_justica'],
                axis=1
            )

            # Para cada ramo tratado (já com a regra aplicada)
            for ramo in df["ramo_tratado"].dropna().unique():
                print(f"\n--- Resultados para {ramo} (arquivo: {nome_arquivo}) ---")
                
                df_ramo = df[df["ramo_tratado"] == ramo]
                metas = metas_por_ramo.get(ramo, [])
                
                for meta in metas:
                    try:
                        resultado = funcoes_metas[meta](df_ramo)
                        print(f"{meta}: {resultado:.2f}%")
                    except Exception as e:
                        print(f"{meta}: erro ao calcular - {e}")
        
        except Exception as e:
            print(f"{nome_arquivo}: erro ao ler o arquivo - {e}")
