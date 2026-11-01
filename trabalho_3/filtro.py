from abc import ABC, abstractmethod
import numpy as np
from scipy.ndimage import convolve, median_filter 

class Filtro(ABC):
    @property
    @abstractmethod
    def halo_depth(self) -> int:
        pass

    @abstractmethod
    def aplicar(self, matriz_com_halo: np.ndarray) -> np.ndarray:
        pass

    def __str__(self) -> str:
        return self.__class__.__name__


class FiltroConvolucional(Filtro):
    def __init__(self, matriz: np.ndarray):
        self.kernel = np.asarray(matriz, dtype=np.float32)
        
        if self.kernel.ndim != 2 or self.kernel.shape[0] % 2 == 0 or self.kernel.shape[1] % 2 == 0:
            raise ValueError("A matriz deve ser 2D e ter tamanho ímpar.")

        self._halo = self.kernel.shape[0] // 2
        self.kernel_3d = self.kernel[:, :, np.newaxis]

    @property
    def halo_depth(self) -> int:
        return self._halo
        
    def aplicar(self, matriz_com_halo: np.ndarray) -> np.ndarray:
        return convolve(
            matriz_com_halo, 
            self.kernel_3d, 
            mode='reflect'
        )

class FiltroMediana(Filtro):
    def __init__(self, tamanho: int = 3):
        if tamanho % 2 == 0:
            raise ValueError("O tamanho da janela de mediana deve ser ímpar.")
            
        self.tamanho = tamanho
        self._halo = tamanho // 2

    @property
    def halo_depth(self) -> int:
        return self._halo
        
    def aplicar(self, matriz_com_halo: np.ndarray) -> np.ndarray:
        return median_filter(
            matriz_com_halo, 
            size=(self.tamanho, self.tamanho, 1),
            mode='reflect',
            output=np.float32
        )
