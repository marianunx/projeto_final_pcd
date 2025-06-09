import pandas as pd
import numpy as np
import glob
from multiprocessing import Pool, cpu_count
import time
import matplotlib.pyplot as plt
from pathlib import Path # Biblioteca padrão para manipular pastas e arquivos

# --- As funções de cálculo e o dicionário METAS_CONFIG não mudam ---
def calcular_meta1(df):
    cnm1 = df['casos_novos_2025'].sum()
    julgadom1 = df['julgados_2025'].sum()
    desm1 = df['dessobrestados_2025'].sum()
    susm1 = df['suspensos_2025'].sum()
    denominador = cnm1 + desm1 - susm1
    return (julgadom1 / denominador) * 100 if denominador > 0 else 0

def calcular_meta_generica(df, col_julgados, col_distribuidos, col_suspensos, fator):
    julgados = df[col_julgados].sum()
    distribuidos = df[col_distribuidos].sum()
    suspensos = df[col_suspensos].sum()
    denominador = distribuidos - suspensos
    if denominador > 0:
        if fator >= 100:
            return (julgados / denominador) * 100
        else:
            return (julgados / denominador) * (1000 / fator)
    return 0

METAS_CONFIG = {
    'Justiça Estadual': {
        'Meta 1': (calcular_meta1,), 'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 8), 'Meta 2B': (calcular_meta_generica, 'julgadom2_b', 'dism2_b', 'susm2_b', 9), 'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 6.5), 'Meta 4B': (calcular_meta_generica, 'julgadom4_b', 'dism4_b', 'susm4_b', 100), 'Meta 6': (calcular_meta_generica, 'julgadom6', 'dism6', 'susm6', 100), 'Meta 8A': (calcular_meta_generica, 'julgadom8_a', 'dism8_a', 'susm8_a', 7.5), 'Meta 8B': (calcular_meta_generica, 'julgadom8_b', 'dism8_b', 'susm8_b', 9),
    }, 'Justiça Federal': {
        'Meta 1': (calcular_meta1,), 'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 8.5), 'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 7), 'Meta 6': (calcular_meta_generica, 'julgadom6', 'dism6', 'susm6', 3.5),
    }, 'Justiça do Trabalho': {
        'Meta 1': (calcular_meta1,), 'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 9.4), 'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 7),
    }, 'Justiça Militar da União': {
        'Meta 1': (calcular_meta1,), 'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 9.5), 'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 9.5),
    }, 'Justiça Militar Estadual': {
        'Meta 1': (calcular_meta1,), 'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 9), 'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 9.5),
    }, 'Tribunal Superior Eleitoral': {
        'Meta 1': (calcular_meta1,), 'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 7), 'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 9),
    }, 'Tribunal Superior do Trabalho': {
        'Meta 1': (calcular_meta1,), 'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 9.5),
    }, 'Superior Tribunal de Justiça': {
        'Meta 1': (calcular_meta1,), 'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 9), 'Meta 6': (calcular_meta_generica, 'julgadom6_a', 'dism6_a', 'susm6_a', 7.5), 'Meta 8': (calcular_meta_generica, 'julgadom8', 'dism8', 'susm8', 10),
    }
}

def processar_tribunal(args):
    df_tribunal, nome_coluna_ramo, nome_coluna_tribunal = args
    try:
        ramo_justica = df_tribunal[nome_coluna_ramo].iloc[0]
        tribunal_nome = df_tribunal[nome_coluna_tribunal].iloc[0]
        resultados = {'Tribunal': tribunal_nome, 'RamoJustica': ramo_justica}
        if ramo_justica in METAS_CONFIG:
            for meta, params in METAS_CONFIG[ramo_justica].items():
                func, *args_func = params
                try:
                    resultados[meta] = func(df_tribunal, *args_func)
                except KeyError:
                    resultados[meta] = 'NA'
        return resultados
    except Exception as e:
        tribunal_nome = df_tribunal[nome_coluna_tribunal].iloc[0] if nome_coluna_tribunal in df_tribunal.columns else "Desconhecido"
        print(f"Erro ao processar dados para o tribunal {tribunal_nome}: {e}")
        return None

# --- FUNÇÃO PRINCIPAL COM GERENCIAMENTO DE PASTAS ---
def main():
    inicio = time.time()

    # --- 1. CONFIGURAÇÃO DAS PASTAS DE ENTRADA E SAÍDA ---
    PASTA_ATUAL = Path.cwd()
    PASTA_ENTRADA = PASTA_ATUAL / 'dados_entrada'
    PASTA_SAIDA = PASTA_ATUAL / 'dados_saida'

    # Cria a pasta de saída, se ela não existir
    PASTA_SAIDA.mkdir(parents=True, exist_ok=True)
    
    # --- 2. Carregamento dos dados da pasta de entrada ---
    print(f"Iniciando carregamento dos arquivos da pasta: '{PASTA_ENTRADA}'")
    # O padrão agora busca por arquivos .csv dentro da pasta_entrada
    caminhos_csv = list(PASTA_ENTRADA.glob('*.csv'))
        
    if not caminhos_csv:
        print(f"Nenhum arquivo CSV encontrado em '{PASTA_ENTRADA}'. Verifique a pasta.")
        return

    lista_dfs = [pd.read_csv(f, sep=None, engine='python', encoding='latin1') for f in caminhos_csv]
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    
    # Salva o arquivo consolidado na pasta de saída
    caminho_consolidado = PASTA_SAIDA / 'Consolidado.csv'
    df_consolidado.to_csv(caminho_consolidado, sep=';', index=False, encoding='latin1')
    print(f"Dados consolidados. Preparando para processamento em memória...")

    # --- 3. Preparação para o processamento paralelo ---
    nome_coluna_ramo = next((col for col in df_consolidado.columns if 'ramo' in col.lower() and 'justica' in col.lower()), None)
    nome_coluna_tribunal = next((col for col in df_consolidado.columns if 'tribunal' in col.lower()), None)
    
    if not nome_coluna_ramo or not nome_coluna_tribunal:
        print("ERRO: Colunas essenciais 'ramo_justica' ou 'tribunal' não encontradas.")
        return

    tarefas = [(group, nome_coluna_ramo, nome_coluna_tribunal) for _, group in df_consolidado.groupby(nome_coluna_tribunal)]

    print(f"Iniciando processamento paralelo em memória de {len(tarefas)} tribunais...")
    # --- 4. Execução paralela ---
    with Pool(cpu_count()) as pool:
        resultados_finais = pool.map(processar_tribunal, tarefas)
    
    # --- 5. Geração dos arquivos de saída na pasta correta ---
    resultados_finais = [r for r in resultados_finais if r is not None]
    df_resumo = pd.DataFrame(resultados_finais)
    df_resumo.fillna('NA', inplace=True)
    
    caminho_resumo = PASTA_SAIDA / 'Resumo Metas.CSV'
    df_resumo.to_csv(caminho_resumo, sep=';', index=False, encoding='latin1')
    
    fim = time.time()
    tempo_total = fim - inicio
    print(f"\n✅ Processamento concluído com sucesso!")
    print(f"   -> Tempo total de execução: {tempo_total:.4f} segundos")

    # --- 6. Geração do gráfico na pasta de saída ---
    caminho_grafico = PASTA_SAIDA / 'grafico_desempenho.pdf'
    
    plt.figure(figsize=(8, 6))
    plt.bar(['Execução Paralela'], [tempo_total], color=['#32CD32'])
    plt.title('Tempo de Execução do Processamento')
    plt.ylabel('Tempo (s)')
    plt.text(0, tempo_total, f'{tempo_total:.2f}s', ha='center', va='bottom', fontweight='bold')
    
    plt.savefig(caminho_grafico)
    print(f"   -> Arquivos salvos em: '{PASTA_SAIDA}'")

if __name__ == '__main__':
    main()