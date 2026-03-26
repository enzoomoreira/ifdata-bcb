from datetime import date, datetime

import pandas as pd

# Tipos para parametros de data
# Aceita int, str, date, datetime, pd.Timestamp, ou lista de qualquer um desses
DateScalar = int | str | date | datetime | pd.Timestamp
DateInput = DateScalar | list[DateScalar]

# Tipos para parametros de conta
# Aceita nome unico ou lista de nomes
AccountInput = str | list[str]

# Tipos para parametros de instituicao
# Aceita CNPJ unico ou lista de CNPJs
InstitutionInput = str | list[str]

__all__ = [
    "DateScalar",
    "DateInput",
    "AccountInput",
    "InstitutionInput",
]
