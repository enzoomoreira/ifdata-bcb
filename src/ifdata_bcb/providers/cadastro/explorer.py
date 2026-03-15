"""
Explorer para dados cadastrais do BCB.

Re-exporta CadastroExplorer do modulo ifdata para manter
a organizacao por namespace.
"""

from ifdata_bcb.providers.ifdata.explorer import CadastroExplorer

__all__ = ["CadastroExplorer"]
