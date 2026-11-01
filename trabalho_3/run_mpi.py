import sys
import time
import numpy as np
from imagem import Imagem
from filtro import FiltroConvolucional, FiltroMediana
from mpi_engine import MPIEngine

def main():
    if len(sys.argv) < 3:
        print("Uso: mpirun -np P python run_mpi.py <filtro> <caminho_imagem>")
        sys.exit(1)

    tipo_filtro = sys.argv[1]
    caminho_imagem = sys.argv[2]

    motor = MPIEngine()

    if motor.rank == 0:
        img = Imagem.from_arquivo(caminho_imagem)
    else:
        img = Imagem(np.zeros((1, 1, 1), dtype=np.float32))

    if tipo_filtro == 'sobel':
        kernel = np.array([[-1, -2, -1], [ 0,  0,  0], [ 1,  2,  1]], dtype=np.float32)
        filtro = FiltroConvolucional(kernel)
    elif tipo_filtro == 'mediana':
        filtro = FiltroMediana(tamanho=5)
    else:
        raise ValueError(f"Filtro desconhecido: {tipo_filtro}")

    motor.comm.Barrier()
    t_inicio = time.time()
    img_resultado = motor.processar(img, filtro)
    motor.comm.Barrier()
    t_fim = time.time()

    if motor.rank == 0:
        print(f"{t_fim - t_inicio:.4f}")

if __name__ == "__main__":
    main()
