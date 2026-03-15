from dataclasses import dataclass


@dataclass(frozen=True)
class ScopeResolution:
    cod_inst: str
    tipo_inst: int
    cnpj_original: str
    escopo: str


__all__ = [
    "ScopeResolution",
]
