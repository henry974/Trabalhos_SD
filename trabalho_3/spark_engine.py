import numpy as np
from pyspark import SparkContext
import gc
from imagem import Imagem
from filtro import Filtro

class SparkEngine:
    def __init__(self, sc: SparkContext, num_particoes: int):
        self.sc = sc
        self.num_particoes = num_particoes

    def processar(self, imagem: Imagem, filtro: Filtro) -> Imagem:
        H = imagem.altura
        halo = filtro.halo_depth
        
        num_particoes = min(self.num_particoes, H)

        linhas_base = H // num_particoes
        resto = H % num_particoes

        fatias_dados = []
        inicio = 0
        
        for i in range(num_particoes):
            tamanho = linhas_base + 1 if i < resto else linhas_base
            fim = inicio + tamanho

            inicio_com_halo = max(0, inicio - halo)
            fim_com_halo = min(H, fim + halo)

            matriz_expandida = imagem.obter_fatia(inicio_com_halo, fim_com_halo)

            corte_topo = inicio - inicio_com_halo
            corte_base = fim_com_halo - fim

            fatias_dados.append((i, matriz_expandida, corte_topo, corte_base))
            inicio = fim

        rdd = self.sc.parallelize(fatias_dados, numSlices=num_particoes)
        del fatias_dados
        def aplicar_filtro_worker(payload):
            idx, matriz_halo, c_topo, c_base = payload
            
            res_com_halo = filtro.aplicar(matriz_halo)
            
            if c_base > 0:
                res_limpo = res_com_halo[c_topo:-c_base, :, :]
            else:
                res_limpo = res_com_halo[c_topo:, :, :]
                
            return (idx, res_limpo)

        rdd_processado = rdd.map(aplicar_filtro_worker)
        imagem.dados = None
        gc.collect()
        resultados_desordenados = rdd_processado.collect()
        resultados_desordenados.sort(key=lambda x: x[0])
        
        matriz_final = np.vstack([res for _, res in resultados_desordenados])
        del resultados_desordenados
        gc.collect()

        imagem.dados = matriz_final
        
        return imagem
