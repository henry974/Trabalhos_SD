import sys
import time
import numpy as np
from pyspark import SparkContext, SparkConf

from imagem import Imagem
from filtro import FiltroConvolucional, FiltroMediana
from spark_engine import SparkEngine

def main():
    if len(sys.argv) < 4:
        print("Uso: python run_spark.py <filtro> <caminho_imagem> <num_particoes> [caminho_saida]", file=sys.stderr)
        sys.exit(1)

    tipo_filtro = sys.argv[1]
    caminho_imagem = sys.argv[2]
    num_particoes = int(sys.argv[3])
    caminho_saida = sys.argv[4] if len(sys.argv) > 4 else None

    conf = SparkConf().setAppName("BenchmarkSpark_Imagem").setMaster(f"local[{num_particoes}]")
    conf.set("spark.driver.memory", "4g")

    sc = SparkContext(conf=conf)
    sc.setLogLevel("OFF")

    img = Imagem.from_arquivo(caminho_imagem)

    if tipo_filtro == 'sobel':
        kernel = np.array([[-1, -2, -1], [ 0,  0,  0], [ 1,  2,  1]], dtype=np.float32)
        filtro = FiltroConvolucional(kernel)
    elif tipo_filtro == 'mediana':
        filtro = FiltroMediana(tamanho=5)
    else:
        sc.stop()
        raise ValueError(f"Filtro desconhecido: {tipo_filtro}")

    motor = SparkEngine(sc, num_particoes)

    t_inicio = time.time()
    img_resultado = motor.processar(img, filtro)
    t_fim = time.time()

    if caminho_saida is not None:
        print(f"[*] Salvando imagem filtrada em: {caminho_saida}...", file=sys.stderr)
        img_resultado.salvar(caminho_saida)

    print(f"{t_fim - t_inicio:.4f}")
    sc.stop()
if __name__ == "__main__":
    main()
