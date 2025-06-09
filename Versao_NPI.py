import pandas as pd
import os
from auxiliar import (
    meta1, meta2A, meta2B, meta2C, meta2ANT, meta4A, meta4B,
    meta6, meta7A, meta7B, meta8A, meta8B, meta10A, meta10B
)

# Caminho da pasta com os arquivos CSV
pasta_dados = 'dados_excel'

# Lista para armazenar os DataFrames
lista_df = []

# Carrega todos os CSVs da pasta
for arquivo in os.listdir(pasta_dados):
    if arquivo.endswith('.csv'):
        caminho_arquivo = os.path.join(pasta_dados, arquivo)
        df = pd.read_csv(caminho_arquivo)
        lista_df.append(df)

# Concatena todos os DataFrames em um único DataFrame
df_total = pd.concat(lista_df, ignore_index=True)

# Lista para armazenar os resultados
resultados = []

# ============================
# Justiça do Trabalho
df_trabalho = df_total[df_total['ramo_justica'] == 'Justiça do Trabalho']
if not df_trabalho.empty:
    resultados.append({'Ramo': 'Justiça do Trabalho', 'Meta': 'Meta 1', 'Resultado (%)': meta1(df_trabalho)})
    resultados.append({'Ramo': 'Justiça do Trabalho', 'Meta': 'Meta 2A', 'Resultado (%)': meta2A(df_trabalho)})
    resultados.append({'Ramo': 'Justiça do Trabalho', 'Meta': 'Meta 2ANT', 'Resultado (%)': meta2ANT(df_trabalho)})
    resultados.append({'Ramo': 'Justiça do Trabalho', 'Meta': 'Meta 4A', 'Resultado (%)': meta4A(df_trabalho)})
    resultados.append({'Ramo': 'Justiça do Trabalho', 'Meta': 'Meta 4B', 'Resultado (%)': meta4B(df_trabalho)})
# ============================
# Justiça Federal
df_federal = df_total[df_total['ramo_justica'] == 'Justiça Federal']
if not df_federal.empty:
    resultados.append({'Ramo': 'Justiça Federal', 'Meta': 'Meta 1', 'Resultado (%)': meta1(df_federal)})
    resultados.append({'Ramo': 'Justiça Federal', 'Meta': 'Meta 2A', 'Resultado (%)': meta2A(df_federal)})
    resultados.append({'Ramo': 'Justiça Federal', 'Meta': 'Meta 2B', 'Resultado (%)': meta2B(df_federal)})
    resultados.append({'Ramo': 'Justiça Federal', 'Meta': 'Meta 2ANT', 'Resultado (%)': meta2ANT(df_federal)})
    resultados.append({'Ramo': 'Justiça Federal', 'Meta': 'Meta 4A', 'Resultado (%)': meta4A(df_federal)})
    resultados.append({'Ramo': 'Justiça Federal', 'Meta': 'Meta 4B', 'Resultado (%)': meta4B(df_federal)})
    resultados.append({'Ramo': 'Justiça Federal', 'Meta': 'Meta 6', 'Resultado (%)': meta6(df_federal)})
    resultados.append({'Ramo': 'Justiça Federal', 'Meta': 'Meta 7A', 'Resultado (%)': meta7A(df_federal)})
    resultados.append({'Ramo': 'Justiça Federal', 'Meta': 'Meta 7B', 'Resultado (%)': meta7B(df_federal)})
    resultados.append({'Ramo': 'Justiça Federal', 'Meta': 'Meta 8A', 'Resultado (%)': meta8A(df_federal)})
    resultados.append({'Ramo': 'Justiça Federal', 'Meta': 'Meta 8B', 'Resultado (%)': meta8B(df_federal)})
    resultados.append({'Ramo': 'Justiça Federal', 'Meta': 'Meta 10A', 'Resultado (%)': meta10A(df_federal)})

# ============================
# Justiça Militar da União
df_militar = df_total[df_total['ramo_justica'] == 'Justiça Militar da União']
if not df_militar.empty:
    resultados.append({'Ramo': 'Justiça Militar da União', 'Meta': 'Meta 1', 'Resultado (%)': meta1(df_militar)})
    resultados.append({'Ramo': 'Justiça Militar da União', 'Meta': 'Meta 2A', 'Resultado (%)': meta2A(df_militar)})
    resultados.append({'Ramo': 'Justiça Militar da União', 'Meta': 'Meta 2B', 'Resultado (%)': meta2B(df_militar)})
    resultados.append({'Ramo': 'Justiça Militar da União', 'Meta': 'Meta 2ANT', 'Resultado (%)': meta2ANT(df_militar)})
    resultados.append({'Ramo': 'Justiça Militar da União', 'Meta': 'Meta 4A', 'Resultado (%)': meta4A(df_militar)})
    resultados.append({'Ramo': 'Justiça Militar da União', 'Meta': 'Meta 4B', 'Resultado (%)': meta4B(df_militar)})

# ============================
# Justiça Militar Estadual 
df_militar_estadual = df_total[df_total['ramo_justica'] == 'Justiça Militar Estadual']
if not df_militar_estadual.empty:
    resultados.append({'Ramo': 'Justiça Militar Estadual', 'Meta': 'Meta 1', 'Resultado (%)': meta1(df_militar_estadual)})
    resultados.append({'Ramo': 'Justiça Militar Estadual', 'Meta': 'Meta 2A', 'Resultado (%)': meta2A(df_militar_estadual)})
    resultados.append({'Ramo': 'Justiça Militar Estadual', 'Meta': 'Meta 2B', 'Resultado (%)': meta2B(df_militar_estadual)})
    resultados.append({'Ramo': 'Justiça Militar Estadual', 'Meta': 'Meta 2ANT', 'Resultado (%)': meta2ANT(df_militar_estadual)})
    resultados.append({'Ramo': 'Justiça Militar Estadual', 'Meta': 'Meta 4A', 'Resultado (%)': meta4A(df_militar_estadual)})
    resultados.append({'Ramo': 'Justiça Militar Estadual', 'Meta': 'Meta 4B', 'Resultado (%)': meta4B(df_militar_estadual)})

# ============================
# Tribunal Superior Eleitoral 
df_tse = df_total[df_total['ramo_justica'] == 'Tribunal Superior Eleitoral']
if not df_tse.empty:
    resultados.append({'Ramo': 'Tribunal Superior Eleitoral', 'Meta': 'Meta 1', 'Resultado (%)': meta1(df_tse)})
    resultados.append({'Ramo': 'Tribunal Superior Eleitoral', 'Meta': 'Meta 2A', 'Resultado (%)': meta2A(df_tse)})
    resultados.append({'Ramo': 'Tribunal Superior Eleitoral', 'Meta': 'Meta 2B', 'Resultado (%)': meta2B(df_tse)})
    resultados.append({'Ramo': 'Tribunal Superior Eleitoral', 'Meta': 'Meta 2ANT', 'Resultado (%)': meta2ANT(df_tse)})
    resultados.append({'Ramo': 'Tribunal Superior Eleitoral', 'Meta': 'Meta 4A', 'Resultado (%)': meta4A(df_tse)})
    resultados.append({'Ramo': 'Tribunal Superior Eleitoral', 'Meta': 'Meta 4B', 'Resultado (%)': meta4B(df_tse)})

# ============================
# Tribunal Superior do Trabalho (TST)
df_tst = df_total[df_total['ramo_justica'] == 'Tribunal Superior do Trabalho']
if not df_tst.empty:
    resultados.append({'Ramo': 'Tribunal Superior do Trabalho', 'Meta': 'Meta 1', 'Resultado (%)': meta1(df_tst)})
    resultados.append({'Ramo': 'Tribunal Superior do Trabalho', 'Meta': 'Meta 2A', 'Resultado (%)': meta2A(df_tst)})
    resultados.append({'Ramo': 'Tribunal Superior do Trabalho', 'Meta': 'Meta 2B', 'Resultado (%)': meta2B(df_tst)})
    resultados.append({'Ramo': 'Tribunal Superior do Trabalho', 'Meta': 'Meta 2ANT', 'Resultado (%)': meta2ANT(df_tst)})
    resultados.append({'Ramo': 'Tribunal Superior do Trabalho', 'Meta': 'Meta 4A', 'Resultado (%)': meta4A(df_tst)})
    resultados.append({'Ramo': 'Tribunal Superior do Trabalho', 'Meta': 'Meta 4B', 'Resultado (%)': meta4B(df_tst)})

# ============================
# Superior Tribunal de Justiça (STJ)
df_stj = df_total[df_total['ramo_justica'] == 'Superior Tribunal de Justiça']
if not df_stj.empty:
    resultados.append({'Ramo': 'Superior Tribunal de Justiça', 'Meta': 'Meta 1', 'Resultado (%)': meta1(df_stj)})
    resultados.append({'Ramo': 'Superior Tribunal de Justiça', 'Meta': 'Meta 2ANT', 'Resultado (%)': meta2ANT(df_stj)})
    resultados.append({'Ramo': 'Superior Tribunal de Justiça', 'Meta': 'Meta 4A', 'Resultado (%)': meta4A(df_stj)})
    resultados.append({'Ramo': 'Superior Tribunal de Justiça', 'Meta': 'Meta 4B', 'Resultado (%)': meta4B(df_stj)})
    resultados.append({'Ramo': 'Superior Tribunal de Justiça', 'Meta': 'Meta 6', 'Resultado (%)': meta6(df_stj)})
    resultados.append({'Ramo': 'Superior Tribunal de Justiça', 'Meta': 'Meta 7A', 'Resultado (%)': meta7A(df_stj)})
    resultados.append({'Ramo': 'Superior Tribunal de Justiça', 'Meta': 'Meta 7B', 'Resultado (%)': meta7B(df_stj)})
    resultados.append({'Ramo': 'Superior Tribunal de Justiça', 'Meta': 'Meta 8A', 'Resultado (%)': meta8A(df_stj)})
    resultados.append({'Ramo': 'Superior Tribunal de Justiça', 'Meta': 'Meta 8B', 'Resultado (%)': meta8B(df_stj)})
    resultados.append({'Ramo': 'Superior Tribunal de Justiça', 'Meta': 'Meta 10A', 'Resultado (%)': meta10A(df_stj)})
    resultados.append({'Ramo': 'Superior Tribunal de Justiça', 'Meta': 'Meta 10B', 'Resultado (%)': meta10B(df_stj)})

# ============================
# Exporta os resultados para CSV
df_resultado = pd.DataFrame(resultados)
df_resultado.to_csv('ResumoMetas.csv', index=False)

print("ResumoMetas.csv gerado com sucesso!")
print("Ramos encontrados nos dados:")
print(df_total['ramo_justica'].unique())
