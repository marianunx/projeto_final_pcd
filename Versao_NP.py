import pandas as pd
import glob
import os
import numpy as np # Para usar np.nan que pode ser útil

# --- Configurações e Constantes ---
# A constante PASTA_DADOS global não é usada ativamente pela função main() neste esqueleto,
# pois main() constrói pasta_dados_reais dinamicamente.

# PASTA_DADOS = 'caminho/para/sua/pasta/com/arquivos_csv/' # Pode comentar ou remover se não for usar.
ARQUIVO_CONSOLIDADO_CSV = 'Consolidado.csv'
ARQUIVO_RESUMO_METAS_CSV = 'ResumoMetas.csv'

# Nomes dos 8 "poderes" como definidos no documento para iterar e aplicar fórmulas.
RAMOS_JUDICIARIO = [
    "Justiça Estadual",
    "Justiça do Trabalho",
    "Justiça Federal",
    "Justiça Militar da União",
    "Justiça Militar Estadual",
    "Tribunal Superior Eleitoral",
    "Tribunal Superior do Trabalho",
    "Superior Tribunal de Justiça"
]

# --- Funções Auxiliares ---

def carregar_e_consolidar_dados(pasta_dados, arquivo_saida_consolidado):
    """
    Lê todos os arquivos CSV de uma pasta que comecem com "teste_" e terminem com ".csv",
    consolida-os e salva em um novo arquivo CSV.
    Retorna o DataFrame consolidado.
    """
    # Procurar por arquivos .csv que comecem com "teste_"
    arquivos_csv = glob.glob(os.path.join(pasta_dados, "teste_*.csv"))

    if not arquivos_csv:
        print(f"Nenhum arquivo CSV encontrado com o padrão 'teste_*.csv' em: {pasta_dados}")
        return pd.DataFrame()

    lista_dfs = []
    for arquivo in arquivos_csv:
        try:
            # Ler o arquivo CSV.

            df = pd.read_csv(arquivo, sep=',', encoding='utf-8') 
            
            lista_dfs.append(df)
            print(f"Arquivo CSV '{arquivo}' lido com sucesso.")
        except Exception as e:
            print(f"Erro ao ler o arquivo CSV '{arquivo}': {e}")
            # Você pode querer adicionar 'continue' aqui para pular arquivos problemáticos
            # ou tratar o erro de forma mais específica.

    if not lista_dfs:
        print("Nenhum DataFrame para consolidar (nenhum arquivo CSV pôde ser lido com sucesso).")
        return pd.DataFrame()

    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    
    try:
        df_consolidado.to_csv(arquivo_saida_consolidado, index=False, sep=';', encoding='utf-8')
        print(f"Arquivo consolidado '{arquivo_saida_consolidado}' salvo com sucesso.")
    except Exception as e:
        print(f"Erro ao salvar o arquivo consolidado: {e}")
        
    return df_consolidado

# --- Funções de Cálculo das Metas (Exemplos/Esqueleto) ---
# Você precisará criar/completar uma função para cada ramo e suas metas.

def calcular_meta1(df_ramo):
    """
    Exemplo para Meta 1: Julgar mais processos que os distribuídos.
    Fórmula: (∑ julgadom1 / (∑ cnm1 + ∑ desm1 - ∑ susm1)) * 100
    Os nomes das colunas (julgadom1, cnm1, etc.) devem ser os EXATOS nos seus arquivos CSV.
    A descrição da Meta 1 no documento diz:
    "Onde cnm1 diz respeito à coluna casos_novos_2025,
    julgadom1 diz respeito à coluna julgados_2025, desm1 diz
    respeito à coluna dessobrestados_2025 e susm1 dis respeito à
    coluna suspensos_2025."
    ENTÃO, USE ESSES NOMES DE COLUNAS:
    """
    cols_necessarias = ['julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025']
    
    # Verificar se todas as colunas necessárias existem no DataFrame do ramo
    for col in cols_necessarias:
        if col not in df_ramo.columns:
            print(f"AVISO (Meta 1): Coluna '{col}' não encontrada para o ramo atual. Meta 1 não será calculada.")
            return np.nan

    # Copiar para evitar SettingWithCopyWarning e converter para numérico
    df_temp = df_ramo[cols_necessarias].copy()
    for col in cols_necessarias:
        df_temp[col] = pd.to_numeric(df_temp[col], errors='coerce')

    # Preencher NaNs resultantes da conversão com 0 para a soma (analise se isso é adequado)
    df_temp = df_temp.fillna(0)

    soma_julgadom1 = df_temp['julgados_2025'].sum()
    soma_cnm1 = df_temp['casos_novos_2025'].sum()
    soma_desm1 = df_temp['dessobrestados_2025'].sum()
    soma_susm1 = df_temp['suspensos_2025'].sum()

    denominador = soma_cnm1 + soma_desm1 - soma_susm1
    if denominador == 0:
        print("AVISO (Meta 1): Denominador zero. Meta 1 não será calculada.")
        return np.nan # Divisão por zero
    
    meta_valor = (soma_julgadom1 / denominador) * 100
    return meta_valor

# --- Justiça Estadual ---
def calcular_metas_justica_estadual(df_justica_estadual):
    resultados = {}
    print(f"  Calculando metas para Justiça Estadual...")
    # Meta 1
    resultados['Meta1'] = calcular_meta1(df_justica_estadual)

    # Meta 2A
    # Fórmula: (∑ julgadom2_a / (∑ dism2_a - ∑ susm2_a)) * (1000 / 8)
    # **IMPORTANTE**: Substitua 'julgadom2_a', 'dism2_a', 'susm2_a' pelos nomes REAIS das colunas
    # que correspondem a esses dados para a Meta2A da Justiça Estadual nos seus arquivos CSV.
    cols_m2a = ['julgadom2_a_NOME_REAL', 'dism2_a_NOME_REAL', 'susm2_a_NOME_REAL'] 
    if all(col in df_justica_estadual.columns for col in cols_m2a):
        df_temp_m2a = df_justica_estadual[cols_m2a].copy()
        for col in cols_m2a: df_temp_m2a[col] = pd.to_numeric(df_temp_m2a[col], errors='coerce')
        df_temp_m2a = df_temp_m2a.fillna(0)

        soma_julgadom2_a = df_temp_m2a[cols_m2a[0]].sum()
        soma_dism2_a = df_temp_m2a[cols_m2a[1]].sum()
        soma_susm2_a = df_temp_m2a[cols_m2a[2]].sum()
        denominador_m2a = soma_dism2_a - soma_susm2_a
        if denominador_m2a == 0:
            resultados['Meta2A'] = np.nan
        else:
            resultados['Meta2A'] = (soma_julgadom2_a / denominador_m2a) * (1000 / 8.0) # Use 8.0 para garantir float
    else:
        resultados['Meta2A'] = np.nan
        print("  AVISO: Colunas para Meta2A da Justiça Estadual não encontradas ou incompletas.")

    # ... Implementar TODAS as outras metas para Justiça Estadual ...
    # Meta 2B, 2C, 2ANT, 4A, 4B, 6, 7A, 7B, 8A, 8B, 10A, 10B
    # Para cada meta, identifique as colunas corretas nos seus CSVs e aplique a fórmula.
    # Use np.nan se uma meta não puder ser calculada (colunas ausentes, denominador zero).

    # Exemplo de como adicionar outras metas (ainda como NA)
    metas_faltantes_je = ['Meta2B', 'Meta2C', 'Meta2ANT', 'Meta4A', 'Meta4B', 'Meta6', 
                          'Meta7A', 'Meta7B', 'Meta8A', 'Meta8B', 'Meta10A', 'Meta10B']
    for meta_f in metas_faltantes_je:
        if meta_f not in resultados: # Só adiciona se não foi calculada (ex: Meta2A)
            resultados[meta_f] = np.nan


    print(f"  Resultados parciais Justiça Estadual: {resultados}")
    return resultados

# ... Crie funções similares para os outros 7 ramos/tribunais ...
# Ex: def calcular_metas_justica_trabalho(df_justica_trabalho): ...
# Lembre-se que cada ramo tem seu próprio conjunto de metas e fórmulas!

# --- Função Principal ---
def main():
    print("Iniciando processamento das Metas do Judiciário...")
    
    # relativa ao local do script.
    script_dir = os.path.dirname(os.path.abspath(__file__)) # Diretório absoluto do script
    pasta_dados_csv = os.path.join(script_dir, 'dados_excel') 
    
    if not os.path.isdir(pasta_dados_csv):
        print(f"ERRO: A pasta de dados '{pasta_dados_csv}' não foi encontrada.")
        print(f"Por favor, crie uma subpasta chamada 'dados_excel' no mesmo local que o script 'Versao_NP.py',")
        print(f"e coloque seus arquivos CSV (teste_*.csv) dentro dela.")
        return

    df_consolidado = carregar_e_consolidar_dados(pasta_dados_csv, ARQUIVO_CONSOLIDADO_CSV)

    if df_consolidado.empty:
        print("Não foi possível carregar ou consolidar os dados. Encerrando.")
        return

    print("\n--- Cabeçalho do DataFrame Consolidado (primeiras 5 linhas) ---")
    print(df_consolidado.head())
    print(f"\n--- Colunas disponíveis no DataFrame Consolidado ---")
    colunas_disponiveis = df_consolidado.columns.tolist()
    print(colunas_disponiveis)
    
    # Verificar valores únicos na coluna 'ramo_justica' para refinar a lógica de filtro
    # ESTA COLUNA É ESSENCIAL. Verifique se ela existe e quais são seus valores.
    if 'ramo_justica' in df_consolidado.columns:
        print("\n--- Valores únicos encontrados em 'ramo_justica' (amostra dos 20 mais frequentes) ---")
        print(df_consolidado['ramo_justica'].value_counts().head(20))
    else:
        print("\nERRO CRÍTICO: Coluna 'ramo_justica' não encontrada no DataFrame consolidado.")
        print("Esta coluna é essencial para separar os dados por tribunal/ramo.")
        print("Verifique seus arquivos CSV: ela deve existir. Se o nome for um pouco diferente, ajuste o código.")
        return

    # 2. Calcular metas para cada ramo/tribunal
    todos_os_resultados_metas = []

    for nome_ramo in RAMOS_JUDICIARIO:
        print(f"\n--- Processando Ramo: {nome_ramo} ---")
        
        # Lógica de filtragem do DataFrame para o ramo atual.
        # Esta parte é CRUCIAL e depende de como os valores na sua coluna 'ramo_justica'
        # identificam cada um dos RAMOS_JUDICIARIO.
        # Você precisará inspecionar os valores da coluna 'ramo_justica' e ajustar.
        
        df_filtrado_ramo = pd.DataFrame() # Inicializa DataFrame vazio para o ramo

        # Exemplo de como você pode filtrar (AJUSTE CONFORME SEUS DADOS!):
        if nome_ramo == "Justiça Estadual":
            identificadores_je = [val for val in df_consolidado['ramo_justica'].unique() if isinstance(val, str) and val.startswith('TJ')]
            if identificadores_je:
                df_filtrado_ramo = df_consolidado[df_consolidado['ramo_justica'].isin(identificadores_je)]
            else: # Se não achar por 'TJ', talvez haja um valor literal
                 df_filtrado_ramo = df_consolidado[df_consolidado['ramo_justica'] == "Justiça Estadual"]
        
        elif nome_ramo == "Superior Tribunal de Justiça":
            df_filtrado_ramo = df_consolidado[df_consolidado['ramo_justica'] == 'STJ'] # Supondo que é 'STJ'
        
        # Adicione 'elif' para cada 'nome_ramo' em RAMOS_JUDICIARIO,
        # ajustando a condição de filtro baseada nos valores reais da sua coluna 'ramo_justica'.
        # Ex:
        # elif nome_ramo == "Justiça do Trabalho":
        #     identificadores_jt = [val for val in df_consolidado['ramo_justica'].unique() if isinstance(val, str) and val.startswith('TRT')]
        #     if identificadores_jt:
        #         df_filtrado_ramo = df_consolidado[df_consolidado['ramo_justica'].isin(identificadores_jt)]
        #     else: # Talvez um valor literal
        #         df_filtrado_ramo = df_consolidado[df_consolidado['ramo_justica'] == "Justiça do Trabalho"]

        # ... (continuar para os outros ramos)

        else:
            print(f"  Lógica de filtro para '{nome_ramo}' ainda não definida. Pulando este ramo por enquanto.")
            # Para garantir que todos os ramos apareçam no CSV final, mesmo sem cálculo:
            # resultados_ramo_vazio = {'Meta_Exemplo': np.nan} # Adicione aqui as metas esperadas para este ramo
            # for meta_vazia, valor_vazio in resultados_ramo_vazio.items():
            #     todos_os_resultados_metas.append({
            #         'RamoJustica': nome_ramo,
            #         'Meta': meta_vazia,
            #         'ValorCalculado': 'NA'
            #     })
            continue # Pula para o próximo ramo se o filtro não foi definido

        if df_filtrado_ramo.empty:
            print(f"  Nenhum dado encontrado para '{nome_ramo}' após o filtro. Verifique a lógica de filtro e os valores na coluna 'ramo_justica'.")
            # Adicionar NAs para todas as metas deste ramo no CSV final
            # Exemplo (você precisa saber quais metas se aplicam a este ramo):
            # if nome_ramo == "Justiça Estadual":
            #    metas_aplicaveis = ['Meta1', 'Meta2A', ...] # Lista de todas as metas da JE
            #    for meta_nome in metas_aplicaveis:
            #         todos_os_resultados_metas.append({'RamoJustica': nome_ramo, 'Meta': meta_nome, 'ValorCalculado': 'NA'})
            continue

        print(f"  Dados filtrados para '{nome_ramo}'. Total de linhas: {len(df_filtrado_ramo)}. Calculando metas...")
        
        resultados_ramo = {}
        if nome_ramo == "Justiça Estadual":
            resultados_ramo = calcular_metas_justica_estadual(df_filtrado_ramo.copy()) # .copy() para evitar SettingWithCopyWarning
        # elif nome_ramo == "Justiça do Trabalho":
        #     resultados_ramo = calcular_metas_justica_trabalho(df_filtrado_ramo.copy())
        # ... Adicionar chamadas para as funções de cálculo dos outros ramos aqui ...
        else:
            print(f"  Função de cálculo de metas para '{nome_ramo}' ainda não implementada ou chamada.")
            # Adicionar NAs para este ramo no CSV final
            # Exemplo: (precisa saber as metas do ramo)
            # metas_deste_ramo = ['Meta1', 'MetaX'] # Lista de metas para este ramo
            # for m_nome in metas_deste_ramo:
            #    todos_os_resultados_metas.append({'RamoJustica': nome_ramo, 'Meta': m_nome, 'ValorCalculado': 'NA'})


        for meta, valor in resultados_ramo.items():
            todos_os_resultados_metas.append({
                'RamoJustica': nome_ramo,
                'Meta': meta,
                'ValorCalculado': valor if pd.notna(valor) else 'NA' # Usar 'NA' string para o CSV
            })

    # 3. Gerar arquivo ResumoMetas.CSV
    if todos_os_resultados_metas:
        df_resumo = pd.DataFrame(todos_os_resultados_metas)
        try:
            df_resumo.to_csv(ARQUIVO_RESUMO_METAS_CSV, index=False, sep=';', encoding='utf-8')
            print(f"\nArquivo de resumo '{ARQUIVO_RESUMO_METAS_CSV}' salvo com sucesso.")
            print("--- Conteúdo do Resumo das Metas ---")
            print(df_resumo)
        except Exception as e:
            print(f"Erro ao salvar o arquivo de resumo '{ARQUIVO_RESUMO_METAS_CSV}': {e}")
    else:
        print("\nNenhum resultado de meta foi calculado para gerar o resumo.")

    # 4. Gerar gráfico (Placeholder - você precisará de matplotlib)
    print("\nPlaceholder para geração do gráfico.")
    # if todos_os_resultados_metas and not df_resumo.empty:
    #     try:
    #         import matplotlib.pyplot as plt
    #         # Exemplo: Gráfico da Meta 1 para os ramos que a calcularam
    #         df_para_grafico = df_resumo[df_resumo['Meta'] == 'Meta1'].copy()
    #         df_para_grafico['ValorCalculadoNum'] = pd.to_numeric(df_para_grafico['ValorCalculado'], errors='coerce')
    #         df_para_grafico.dropna(subset=['ValorCalculadoNum'], inplace=True) # Remover NAs numéricos

    #         if not df_para_grafico.empty:
    #             plt.figure(figsize=(12, 8))
    #             plt.bar(df_para_grafico['RamoJustica'], df_para_grafico['ValorCalculadoNum'], color='skyblue')
    #             plt.xlabel("Ramo da Justiça")
    #             plt.ylabel("Valor da Meta 1 (%)")
    #             plt.title("Desempenho da Meta 1 por Ramo da Justiça")
    #             plt.xticks(rotation=45, ha="right")
    #             plt.tight_layout()
    #             plt.savefig("grafico_meta1.png")
    #             print("Gráfico 'grafico_meta1.png' salvo (exemplo).")
    #         else:
    #             print("Sem dados válidos para gerar o gráfico da Meta 1.")
    #     except ImportError:
    #         print("Biblioteca matplotlib não encontrada. Gráfico não gerado. Instale com: pip install matplotlib")
    #     except Exception as e:
    #         print(f"Erro ao gerar o gráfico: {e}")


    print("\nProcessamento não paralelo concluído.")

if __name__ == '__main__':
    main()