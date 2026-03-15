# Tipos para parametros de data
# Aceita int (202412), str ('2024-12', '202412'), ou lista de ambos
DateInput = int | str | list[int] | list[str]

# Tipos para parametros de conta
# Aceita nome unico ou lista de nomes
AccountInput = str | list[str]

# Tipos para parametros de instituicao
# Aceita CNPJ unico ou lista de CNPJs
InstitutionInput = str | list[str]

__all__ = [
    "DateInput",
    "AccountInput",
    "InstitutionInput",
]
