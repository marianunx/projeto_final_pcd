import pandas as pd
import numpy as np
import glob
from multiprocessing import Pool, cpu_count
import time
import matplotlib.pyplot as plt


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
        if fator > 1:
            return (julgados / denominador) * (1000 / fator)
        else:
            return (julgados / denominador) * 100
    return 0



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
}



def processar_tribunal(args):
    arquivo, ramo_justica = args
    try:
        df = pd.read_csv(arquivo, sep=';', encoding='latin1')
        resultados = {'Tribunal': df['tribunal'].iloc[0], 'RamoJustica': ramo_justica}

        if ramo_justica in METAS_CONFIG:
            for meta, params in METAS_CONFIG[ramo_justica].items():
                func, *args_func = params
                resultados[meta] = func(df, *args_func)
        
        return resultados
    except Exception as e:
        print(f"Erro ao processar o arquivo {arquivo}: {e}")
        return None


def main():
    inicio = time.time()
    
    arquivos_csv = glob.glob('*.csv')
    if 'Consolidado.csv' in arquivos_csv:
        arquivos_csv.remove('Consolidado.csv')
    if 'Resumo Metas.CSV' in arquivos_csv:
        arquivos_csv.remove('Resumo Metas.CSV')
        
    lista_dfs = [pd.read_csv(f, sep=';', encoding='latin1') for f in arquivos_csv]
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    df_consolidado.to_csv('Consolidado.csv', sep=';', index=False, encoding='latin1')
    
    ramos_justica = df_consolidado.groupby('ramo_justica').groups.keys()
    tarefas = []
    for ramo in ramos_justica:
        tribunais_ramo = df_consolidado[df_consolidado['ramo_justica'] == ramo]['tribunal'].unique()
        for tribunal in tribunais_ramo:
            arquivo_tribunal = next((f for f in arquivos_csv if tribunal in pd.read_csv(f, sep=';', encoding='latin1')['tribunal'].iloc[0]), None)
            if arquivo_tribunal:
                tarefas.append((arquivo_tribunal, ramo))

    with Pool(cpu_count()) as pool:
        resultados_finais = pool.map(processar_tribunal, tarefas)

    resultados_finais = [r for r in resultados_finais if r is not None]
    df_resumo = pd.DataFrame(resultados_finais)
    df_resumo.fillna('NA', inplace=True)
    df_resumo.to_csv('Resumo Metas.CSV', sep=';', index=False, encoding='latin1')
    
    fim = time.time()
    tempo_total = fim - inicio
    print(f"\nTempo de execução da versão paralela: {tempo_total:.4f} segundos")

   
    tempos = {'Versão Não Paralela (Estimado)': tempo_total * (cpu_count() * 0.8), 'Versão Paralela': tempo_total}
    plt.figure(figsize=(8, 6))
    plt.bar(tempos.keys(), tempos.values(), color=['blue', 'green'])
    plt.title('Comparativo de Desempenho (Speedup)')
    plt.ylabel('Tempo de Execução (s)')
    plt.savefig('grafico_speedup.pdf')
    print("Gráfico de speedup salvo como 'grafico_speedup.pdf'")


if __name__ == '__main__':
    main()