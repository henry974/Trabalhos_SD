import numpy as np
from PIL import Image

class Imagem:
    def __init__(self, dados: np.ndarray):
        arr = np.ascontiguousarray(dados, dtype=np.float32)
        if arr.ndim == 2:
            self.dados = arr[:, :, np.newaxis]
        elif arr.ndim == 3:
            self.dados = arr
        else:
            raise ValueError(f"Matriz de imagem deve ser 2D ou 3D. Recebido: {arr.ndim}D")

    @property
    def altura(self) -> int:
        return self.dados.shape[0]

    @property
    def largura(self) -> int:
        return self.dados.shape[1]

    @property
    def canais(self) -> int:
        return self.dados.shape[2]

    def obter_fatia(self, linha_inicio: int, linha_fim: int) -> np.ndarray:
        if linha_inicio < 0 or linha_fim > self.altura or linha_inicio >= linha_fim:
            raise ValueError(f"Limites inválidos: {linha_inicio} até {linha_fim} (Altura total: {self.altura})")
            
        return self.dados[linha_inicio:linha_fim, :, :]

    def substituir_fatia(self, linha_inicio: int, linha_fim: int, fatia: np.ndarray):
        if fatia.shape[0] != (linha_fim - linha_inicio):
            raise ValueError("O tamanho da fatia recebida não bate com os limites de substituição.")
            
        self.dados[linha_inicio:linha_fim, :, :] = fatia

    @classmethod
    def from_arquivo(cls, caminho: str):
        Image.MAX_IMAGE_PIXELS = None
        
        with Image.open(caminho) as img:
            if img.mode in ('RGBA', 'LA', 'P', 'CMYK'):
                img = img.convert('RGB')
                
            arr_bruto = np.array(img)
            
            if arr_bruto.dtype in (np.float32, np.float64):
                dados = arr_bruto.astype(np.float32)
                del arr_bruto
            elif arr_bruto.dtype in (np.uint16, np.int32) or img.mode.startswith('I'):
                dados = arr_bruto.astype(np.float32)
                del arr_bruto
                dados /= 65535.0
            else:
                dados = arr_bruto.astype(np.float32)
                del arr_bruto
                dados /= 255.0
            
            return cls(dados)

    def salvar(self, caminho: str):
        dados_norm = np.clip(self.dados, 0.0, 1.0)
        
        dados_uint8 = (dados_norm * 255.0).astype(np.uint8)
        
        if self.canais == 1:
            img_pil = Image.fromarray(dados_uint8[:, :, 0], mode='L')
        elif self.canais == 3:
            img_pil = Image.fromarray(dados_uint8, mode='RGB')
        elif self.canais == 4:
            img_pil = Image.fromarray(dados_uint8, mode='RGBA')
        else:
            img_pil = Image.fromarray(dados_uint8[:, :, :3], mode='RGB')            
        img_pil.save(caminho)
