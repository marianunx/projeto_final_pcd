import os
import pandas as pd

pasta = './Dados'

saida = 'Consolidado.csv'

dataframes = []

for arquivo in os.listdir(pasta):
    if arquivo.endswith('.csv') and arquivo != saida:
        print(f"Lendo {arquivo}")
        df = pd.read_csv(os.path.join(pasta, arquivo))
        dataframes.append(df)

consolidado = pd.concat(dataframes, ignore_index=True)

consolidado.to_csv(saida, index=False)
print(f"Arquivo '{saida}' criado com sucesso!")
