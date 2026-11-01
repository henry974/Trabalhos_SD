import numpy as np
from mpi4py import MPI
from imagem import Imagem
from filtro import Filtro

class MPIEngine:
    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()

    def processar(self, imagem: Imagem, filtro: Filtro) -> Imagem:
        W = imagem.largura if self.rank == 0 else 0
        C = imagem.canais if self.rank == 0 else 0
        H = imagem.altura if self.rank == 0 else 0
        
        W = self.comm.bcast(W, root=0)
        C = self.comm.bcast(C, root=0)
        H = self.comm.bcast(H, root=0)

        counts_linhas = []
        deslocamentos_linhas = []
        
        if self.rank == 0:
            linhas_base = H // self.size
            resto = H % self.size
            
            deslocamento_atual = 0
            for i in range(self.size):
                linhas = linhas_base + 1 if i < resto else linhas_base
                counts_linhas.append(linhas)
                deslocamentos_linhas.append(deslocamento_atual)
                deslocamento_atual += linhas
                
            counts_elementos = [l * W * C for l in counts_linhas]
            deslocamentos_elementos = [d * W * C for d in deslocamentos_linhas]
            dados_planos = imagem.dados.ravel()
        else:
            counts_linhas = None
            counts_elementos = None
            deslocamentos_elementos = None
            dados_planos = None

        minhas_linhas = self.comm.scatter(counts_linhas, root=0)

        fatia_local = np.empty((minhas_linhas, W, C), dtype=np.float32)

        self.comm.Scatterv(
            [dados_planos, counts_elementos, deslocamentos_elementos, MPI.FLOAT],
            fatia_local,
            root=0
        )

        halo = filtro.halo_depth
        
        if halo > 0:
            linhas_com_halo = minhas_linhas
            tem_vizinho_cima = self.rank > 0
            tem_vizinho_baixo = self.rank < self.size - 1
            
            if tem_vizinho_cima:  linhas_com_halo += halo
            if tem_vizinho_baixo: linhas_com_halo += halo
                
            fatia_com_halo = np.empty((linhas_com_halo, W, C), dtype=np.float32)
            
            idx_inicio_dados = halo if tem_vizinho_cima else 0
            idx_fim_dados = idx_inicio_dados + minhas_linhas
            fatia_com_halo[idx_inicio_dados:idx_fim_dados, :, :] = fatia_local
            
            requisicoes = []
            
            if tem_vizinho_cima:
                req_recv_top = self.comm.Irecv(fatia_com_halo[0:halo, :, :], source=self.rank-1, tag=11)
                requisicoes.append(req_recv_top)
            
            if tem_vizinho_baixo:
                req_send_bottom = self.comm.Isend(fatia_local[-halo:, :, :], dest=self.rank+1, tag=11)
                requisicoes.append(req_send_bottom)
                
            if tem_vizinho_baixo:
                req_recv_bot = self.comm.Irecv(fatia_com_halo[-halo:, :, :], source=self.rank+1, tag=22)
                requisicoes.append(req_recv_bot)
                
            if tem_vizinho_cima:
                req_send_top = self.comm.Isend(fatia_local[0:halo, :, :], dest=self.rank-1, tag=22)
                requisicoes.append(req_send_top)

            MPI.Request.Waitall(requisicoes)
        else:
            fatia_com_halo = fatia_local
            idx_inicio_dados = 0
            idx_fim_dados = minhas_linhas

        res_com_halo = filtro.aplicar(fatia_com_halo)

        res_limpo = res_com_halo[idx_inicio_dados:idx_fim_dados, :, :]

        res_plano_local = np.ascontiguousarray(res_limpo).ravel()
        imagem_final_plana = None
        
        if self.rank == 0:
            imagem_final_plana = np.empty(H * W * C, dtype=np.float32)

        if self.rank == 0:
            recvbuf = [imagem_final_plana, counts_elementos, deslocamentos_elementos, MPI.FLOAT]
        else:
            recvbuf = None

        self.comm.Gatherv(sendbuf=res_plano_local, recvbuf=recvbuf, root=0)

        if self.rank == 0:
            matriz_final = imagem_final_plana.reshape((H, W, C))
            imagem.substituir_fatia(0, H, matriz_final)
            
        self.comm.Barrier()
        
        return imagem if self.rank == 0 else None            
