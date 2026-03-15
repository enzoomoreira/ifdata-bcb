"""
Provider Cadastro - Exploracao de dados cadastrais do IFDATA.

Nota: Cadastro usa IFDATACadastroCollector para coleta.
O CadastroExplorer e uma classe distinta para consultas.
"""

from ifdata_bcb.providers.cadastro.explorer import CadastroExplorer

__all__ = ["CadastroExplorer"]
