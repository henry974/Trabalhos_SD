import argparse
import subprocess
import csv
import os
import sys
import pandas as pd
import matplotlib.pyplot as plt

FILTROS = ['sobel', 'mediana']
PROCESSOS = [1, 2, 4, 8, 16]
ENGINES = ['mpi', 'spark']
ARQUIVO_CSV = 'resultados/resultados_benchmark.csv'

cores = {
    ('mpi', 'sobel'): '#1f77b4',       # Azul padrão
    ('mpi', 'mediana'): '#2ca02c',     # Verde
    ('spark', 'sobel'): '#ff7f0e',     # Laranja
    ('spark', 'mediana'): '#d62728'    # Vermelho
}

marcadores = {
    'mpi': 'o',
    'spark': '^'
}

nomes_engine = {
    'mpi': 'OpenMPI',
    'spark': 'Apache Spark'
}

def rodar_benchmark(imagens):
    """Executa as baterias MPI e Spark via subprocess e recolhe os tempos."""
    os.makedirs(os.path.dirname(ARQUIVO_CSV), exist_ok=True)
    
    py_exec = sys.executable

    with open(ARQUIVO_CSV, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Imagem', 'Engine', 'Filtro', 'Processos', 'Tempo (s)'])

        for caminho_imagem in imagens:
            nome_img = os.path.basename(caminho_imagem)
            print(f"\n[{nome_img.upper()}] Iniciando bateria de testes...")
            
            for engine in ENGINES:
                print(f"--- Motor: {engine.upper()} ---")
                for filtro in FILTROS:
                    for p in PROCESSOS:
                        print(f"  -> {filtro.upper()} com {p} processo(s)... ", end="", flush=True)
                        
                        if engine == 'mpi':
                            comando = [
                                "mpirun", 
                                "--use-hwthread-cpus", 
                                "-np", str(p), 
                                py_exec, "run_mpi.py", 
                                filtro, caminho_imagem
                            ]
                        else:
                            comando = [
                                py_exec, "run_spark.py",
                                filtro, caminho_imagem, str(p)
                            ]
                        
                        try:
                            resultado = subprocess.run(comando, capture_output=True, text=True, check=True)
                            
                            linhas = [l.strip() for l in resultado.stdout.strip().split('\n') if l.strip()]
                            tempo = None
                            for l in reversed(linhas):
                                try:
                                    tempo = float(l)
                                    break
                                except ValueError:
                                    continue
                            
                            if tempo is not None:
                                print(f"{tempo:.2f}s")
                                writer.writerow([nome_img, engine, filtro, p, tempo])
                                f.flush()
                            else:
                                print(f"FALHOU! Não foi encontrado valor numérico no stdout.")
                                if resultado.stderr:
                                    print("--- STDERR ---")
                                    print(resultado.stderr)
                                
                        except subprocess.CalledProcessError as e:
                            print("FALHOU NO SUBPROCESS!")
                            if e.stderr:
                                print(e.stderr)
                            continue

def plotar_graficos(imagens):
    """Lê o CSV e gera:
    1. 2 gráficos de Tempo vs Processos (1 Sobel com 2 linhas, 1 Mediana com 2 linhas).
    2. 2 gráficos de Eficiência vs Processos (1 Sobel com 2 linhas, 1 Mediana com 2 linhas).
    3. 2 gráficos completos lado a lado (Tempo + Eficiência: 1 Sobel com 2 linhas em cada subplot, 1 Mediana).
    """
    if not os.path.exists(ARQUIVO_CSV):
        print(f"Erro: Ficheiro {ARQUIVO_CSV} não encontrado.")
        return

    df = pd.read_csv(ARQUIVO_CSV)
    os.makedirs('resultados', exist_ok=True)
    
    df['Engine'] = df['Engine'].astype(str).str.strip().str.lower().replace({
        'openmpi': 'mpi',
        'apache spark': 'spark',
        'pyspark': 'spark'
    })
    df['Filtro'] = df['Filtro'].astype(str).str.strip().str.lower()
    df['Imagem'] = df['Imagem'].astype(str).str.strip()
    
    for caminho_imagem in imagens:
        nome_img = os.path.basename(caminho_imagem)
        slug_img = os.path.splitext(nome_img)[0]
        
        dados_img = df[(df['Imagem'] == nome_img) | (df['Imagem'] == caminho_imagem)]
        
        if dados_img.empty:
            print(f"Aviso: Nenhum dado encontrado para a imagem '{nome_img}' no CSV.")
            continue

        metricas = {}
        for engine in ENGINES:
            for filtro in FILTROS:
                sub = dados_img[(dados_img['Engine'] == engine) & (dados_img['Filtro'] == filtro)].copy()
                if not sub.empty:
                    sub['Processos'] = pd.to_numeric(sub['Processos'], errors='coerce')
                    sub['Tempo (s)'] = pd.to_numeric(sub['Tempo (s)'], errors='coerce')
                    sub = sub.dropna(subset=['Processos', 'Tempo (s)']).sort_values(by='Processos')
                    
                    if not sub.empty:
                        p = sub['Processos'].values
                        t = sub['Tempo (s)'].values
                        t1 = t[0]
                        speedup = t1 / t
                        eficiencia = speedup / p
                        metricas[(engine, filtro)] = {
                            'p': p,
                            't': t,
                            'eficiencia': eficiencia
                        }
                else:
                    print(f"Aviso: Dados ausentes para {engine.upper()} com filtro {filtro.upper()} na imagem '{nome_img}'.")

        for filtro in FILTROS:
            plt.figure(figsize=(7, 5))
            linhas_desenhadas = 0
            
            for engine in ENGINES:
                if (engine, filtro) in metricas:
                    d = metricas[(engine, filtro)]
                    cor = cores.get((engine, filtro), 'black')
                    rotulo = f"{nomes_engine[engine]} ({filtro.capitalize()})"
                    plt.plot(d['p'], d['t'], 
                             marker=marcadores[engine], 
                             color=cor, 
                             linewidth=2.5, 
                             markersize=8, 
                             label=rotulo)
                    linhas_desenhadas += 1
            
            plt.title(f'Tempo de Execução - Filtro {filtro.capitalize()} ({nome_img})', fontsize=12, fontweight='bold')
            plt.xlabel('Número de Processos (P)', fontsize=11)
            plt.ylabel('Tempo (s)', fontsize=11)
            plt.xticks(PROCESSOS)
            plt.grid(True, linestyle=':', alpha=0.6)
            plt.legend(frameon=True)
            plt.tight_layout()
            
            saida = f'resultados/tempo_{filtro}_{slug_img}.png'
            plt.savefig(saida, dpi=300)
            plt.close()
            print(f"[!] Gráfico de Tempo salvo ({linhas_desenhadas} linhas): {saida}")

        for filtro in FILTROS:
            plt.figure(figsize=(7, 5))
            plt.axhline(1.0, color='gray', linestyle='--', linewidth=1.2, alpha=0.8, label='Ideal (1.0)')
            linhas_desenhadas = 0
            
            for engine in ENGINES:
                if (engine, filtro) in metricas:
                    d = metricas[(engine, filtro)]
                    cor = cores.get((engine, filtro), 'black')
                    rotulo = f"{nomes_engine[engine]} ({filtro.capitalize()})"
                    plt.plot(d['p'], d['eficiencia'], 
                             marker=marcadores[engine], 
                             color=cor, 
                             linewidth=2.5, 
                             markersize=8, 
                             label=rotulo)
                    linhas_desenhadas += 1
            
            plt.title(f'Eficiência Efetiva - Filtro {filtro.capitalize()} ({nome_img})', fontsize=12, fontweight='bold')
            plt.xlabel('Número de Processos (P)', fontsize=11)
            plt.ylabel('Uso Efetivo dos Recursos (0 a 1)', fontsize=11)
            plt.xticks(PROCESSOS)
            plt.ylim(0, 1.1)
            plt.grid(True, linestyle=':', alpha=0.6)
            plt.legend(frameon=True)
            plt.tight_layout()
            
            saida = f'resultados/eficiencia_{filtro}_{slug_img}.png'
            plt.savefig(saida, dpi=300)
            plt.close()
            print(f"[!] Gráfico de Eficiência salvo ({linhas_desenhadas} linhas): {saida}")

        for filtro in FILTROS:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
            
            # Subplot 1: Tempo
            for engine in ENGINES:
                if (engine, filtro) in metricas:
                    d = metricas[(engine, filtro)]
                    cor = cores.get((engine, filtro), 'black')
                    rotulo = f"{nomes_engine[engine]} ({filtro.capitalize()})"
                    ax1.plot(d['p'], d['t'], 
                             marker=marcadores[engine], 
                             color=cor, 
                             linewidth=2.5, 
                             markersize=8, 
                             label=rotulo)
            ax1.set_title(f'Tempo de Execução - {filtro.capitalize()}', fontsize=12, fontweight='bold')
            ax1.set_xlabel('Número de Processos (P)', fontsize=11)
            ax1.set_ylabel('Tempo (s)', fontsize=11)
            ax1.set_xticks(PROCESSOS)
            ax1.grid(True, linestyle=':', alpha=0.6)
            ax1.legend(frameon=True)

            # Subplot 2: Eficiência
            ax2.axhline(1.0, color='gray', linestyle='--', linewidth=1.2, alpha=0.8, label='Ideal (1.0)')
            for engine in ENGINES:
                if (engine, filtro) in metricas:
                    d = metricas[(engine, filtro)]
                    cor = cores.get((engine, filtro), 'black')
                    rotulo = f"{nomes_engine[engine]} ({filtro.capitalize()})"
                    ax2.plot(d['p'], d['eficiencia'], 
                             marker=marcadores[engine], 
                             color=cor, 
                             linewidth=2.5, 
                             markersize=8, 
                             label=rotulo)
            ax2.set_title(f'Eficiência - {filtro.capitalize()}', fontsize=12, fontweight='bold')
            ax2.set_xlabel('Número de Processos (P)', fontsize=11)
            ax2.set_ylabel('Uso Efetivo dos Recursos (0 a 1)', fontsize=11)
            ax2.set_xticks(PROCESSOS)
            ax2.set_ylim(0, 1.1)
            ax2.grid(True, linestyle=':', alpha=0.6)
            ax2.legend(frameon=True)

            plt.suptitle(f'Comparativo OpenMPI vs Apache Spark - Filtro {filtro.capitalize()} ({nome_img})', fontsize=14, fontweight='bold')
            plt.tight_layout()
            
            saida = f'resultados/completo_{filtro}_{slug_img}.png'
            plt.savefig(saida, dpi=300)
            plt.close()
            print(f"[!] Gráfico Completo salvo: {saida}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark Distribuído Comparativo: MPI vs Apache Spark.")
    parser.add_argument(
        "imagens", 
        nargs="+", 
        help="Caminho para uma ou mais imagens reais (ex: imagens/satelite.tif)"
    )
    args = parser.parse_args()

    rodar_benchmark(args.imagens)
    plotar_graficos(args.imagens)
