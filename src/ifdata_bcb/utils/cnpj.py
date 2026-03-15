"""
Utilitários para padronização de CNPJ
"""

import pandas as pd
import re
from typing import Union


def standardize_cnpj_base8(cnpj_input: Union[str, pd.Series]) -> Union[str, pd.Series]:
    """
    Padroniza um CNPJ ou código para uma string de 8 dígitos, lidando com
    diferentes formatos de entrada.
    
    Args:
        cnpj_input: String ou pandas Series contendo CNPJ(s) a serem padronizados
        
    Returns:
        String ou pandas Series com CNPJ(s) padronizados em 8 dígitos
    """
    def _process_single_cnpj(cnpj_element_val):
        if pd.isna(cnpj_element_val):
            return None
        cleaned = re.sub(r'[^0-9]', '', str(cnpj_element_val).strip())
        if not cleaned:
            return None
        return cleaned.zfill(8)[:8]

    if isinstance(cnpj_input, pd.Series):
        return cnpj_input.apply(_process_single_cnpj)
    
    return _process_single_cnpj(cnpj_input)

