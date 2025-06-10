import pandas as pd
import numpy as np
import glob
from multiprocessing import Pool, cpu_count
import time
import matplotlib.pyplot as plt
from pathlib import Path

# === Funções de cálculo das metas ===
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

# === Configuração das metas por ramo de justiça ===
METAS_CONFIG = {
    'Justiça Estadual': {
        'Meta 1': (calcular_meta1,),
        'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 8),
        'Meta 2B': (calcular_meta_generica, 'julgadom2_b', 'dism2_b', 'susm2_b', 9),
        'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 6.5),
        'Meta 4B': (calcular_meta_generica, 'julgadom4_b', 'dism4_b', 'susm4_b', 100),
        'Meta 6': (calcular_meta_generica, 'julgadom6', 'dism6', 'susm6', 100),
        'Meta 8A': (calcular_meta_generica, 'julgadom8_a', 'dism8_a', 'susm8_a', 7.5),
        'Meta 8B': (calcular_meta_generica, 'julgadom8_b', 'dism8_b', 'susm8_b', 9),
    },
    'Justiça Federal': {
        'Meta 1': (calcular_meta1,),
        'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 8.5),
        'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 7),
        'Meta 6': (calcular_meta_generica, 'julgadom6', 'dism6', 'susm6', 3.5),
    },
    'Justiça do Trabalho': {
        'Meta 1': (calcular_meta1,),
        'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 9.4),
        'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 7),
    },
    'Justiça Militar da União': {
        'Meta 1': (calcular_meta1,),
        'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 9.5),
        'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 9.5),
    },
    'Justiça Militar Estadual': {
        'Meta 1': (calcular_meta1,),
        'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 9),
        'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 9.5),
    },
    'Tribunal Superior Eleitoral': {
        'Meta 1': (calcular_meta1,),
        'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 7),
        'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 9),
    },
    'Tribunal Superior do Trabalho': {
        'Meta 1': (calcular_meta1,),
        'Meta 2A': (calcular_meta_generica, 'julgadom2_a', 'dism2_a', 'susm2_a', 9.5),
    },
    'Superior Tribunal de Justiça': {
        'Meta 1': (calcular_meta1,),
        'Meta 4A': (calcular_meta_generica, 'julgadom4_a', 'dism4_a', 'susm4_a', 9),
        'Meta 6': (calcular_meta_generica, 'julgadom6_a', 'dism6_a', 'susm6_a', 7.5),
        'Meta 8': (calcular_meta_generica, 'julgadom8', 'dism8', 'susm8', 10),
    }
}

# === Função para processar um tribunal ===
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
        print(f"Erro ao processar tribunal: {e}")
        return None

# === Funções para medição de tempo ===
def processar_tarefas(tarefas, paralelo=True):
    if paralelo:
        with Pool(cpu_count()) as pool:
            return [r for r in pool.map(processar_tribunal, tarefas) if r is not None]
    else:
        return [r for r in map(processar_tribunal, tarefas) if r is not None]

def medir_tempo_execucao(tarefas, paralelo=True):
    inicio = time.time()
    resultados = processar_tarefas(tarefas, paralelo=paralelo)
    tempo_total = time.time() - inicio
    return resultados, tempo_total

# === Função principal ===
def main():
    PASTA_ATUAL = Path.cwd()
    PASTA_ENTRADA = PASTA_ATUAL / 'dados_entrada'
    PASTA_SAIDA = PASTA_ATUAL / 'dados_saida'
    PASTA_SAIDA.mkdir(exist_ok=True)

    caminhos_csv = list(PASTA_ENTRADA.glob('*.csv'))
    if not caminhos_csv:
        print("Nenhum CSV encontrado em 'dados_entrada'.")
        return

    lista_dfs = [pd.read_csv(f, sep=None, engine='python', encoding='latin1') for f in caminhos_csv]
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    df_consolidado.to_csv(PASTA_SAIDA / 'Consolidado.csv', sep=';', index=False, encoding='latin1')

    nome_coluna_ramo = next((c for c in df_consolidado.columns if 'ramo' in c.lower()), None)
    nome_coluna_tribunal = next((c for c in df_consolidado.columns if 'tribunal' in c.lower()), None)
    if not nome_coluna_ramo or not nome_coluna_tribunal:
        print("Colunas 'ramo' ou 'tribunal' não encontradas.")
        return

    tarefas = [(group, nome_coluna_ramo, nome_coluna_tribunal) for _, group in df_consolidado.groupby(nome_coluna_tribunal)]

    print("Executando versão SEQUENCIAL...")
    resultados_seq, tempo_seq = medir_tempo_execucao(tarefas, paralelo=False)

    print("Executando versão PARALELA...")
    resultados_par, tempo_par = medir_tempo_execucao(tarefas, paralelo=True)

    df_resumo = pd.DataFrame(resultados_par).fillna('NA')
    df_resumo.to_csv(PASTA_SAIDA / 'Resumo Metas.CSV', sep=';', index=False, encoding='latin1')

    speedup = tempo_seq / tempo_par if tempo_par > 0 else 0

    # === Gráfico ===
    plt.figure(figsize=(8, 6))
    tempos = [tempo_seq, tempo_par]
    nomes = ['Sequencial', 'Paralelo']
    cores = ['#1f77b4', '#2ca02c']
    plt.bar(nomes, tempos, color=cores)
    plt.title(f'Tempo de Execução (Speedup = {speedup:.2f}x)')
    plt.ylabel('Tempo (s)')
    for i, t in enumerate(tempos):
        plt.text(i, t, f'{t:.2f}s', ha='center', va='bottom', fontweight='bold')
    plt.tight_layout()
    plt.savefig(PASTA_SAIDA / 'grafico_desempenho.pdf')

    print("\n✅ Execução concluída com sucesso!")
    print(f"Tempo Sequencial: {tempo_seq:.4f} segundos")
    print(f"Tempo Paralelo:   {tempo_par:.4f} segundos")
    print(f"Speedup:          {speedup:.2f}x")
    print(f"Gráfico salvo em: {PASTA_SAIDA / 'grafico_desempenho.pdf'}")

if __name__ == '__main__':
    main()
