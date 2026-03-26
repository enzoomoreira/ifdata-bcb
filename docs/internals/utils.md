# Utilitarios

O modulo `utils` fornece funcoes e classes utilitarias reutilizaveis.

## Localizacao

```
src/ifdata_bcb/utils/
|-- __init__.py           # Exports publicos
|-- text.py              # Normalizacao de texto
|-- date.py              # Processamento de datas
|-- fuzzy.py             # Busca fuzzy
|-- cnpj.py              # Padronizacao de CNPJ
|-- nulls.py             # Check escalar de nulidade (is_valid)
+-- period.py            # Extracao de periodos de arquivos
```

---

## text.py

### normalize_accents()

Remove acentos e diacriticos de texto:

```python
def normalize_accents(text: str) -> str:
    """
    Remove acentos usando decomposicao Unicode NFKD.

    Exemplos:
        "Sao Paulo" -> "Sao Paulo"
        "Itau" -> "Itau"
        "Para" -> "Para"

    Se entrada nao e string, retorna como-esta.
    """
```

**Algoritmo**:
1. Aplica normalizacao Unicode NFKD (separa base de modificadores)
2. Filtra caracteres nao-combinadores (`unicodedata.combining()`)

### stem_ptbr()

Stemming simples PT-BR para busca singular/plural:

```python
def stem_ptbr(term: str) -> str:
    """
    Remove sufixos comuns para unificar formas singular/plural.

    Usa pares atomicos (singular, plural) com raiz minima de 4 chars
    para evitar falsos positivos.

    Exemplos:
        "operacao"  -> "opera"   (match com "operacoes")
        "captacao"  -> "capta"   (match com "captacoes")
        "capital"   -> "capit"   (match com "capitais")
        "credito"   -> "credito" (sem inflexao, passthrough)
        "cao"       -> "cao"     (raiz < 4 chars, passthrough)
    """
```

**Pares suportados**: `(icao, icoes)`, `(ucao, ucoes)`, `(cao, coes)`, `(sao, soes)`, `(ao, oes)`, `(al, ais)`, `(el, eis)`.

Usado internamente por `list_contas()` no IFDATA e COSIF para stemming do termo de busca.

### format_entity_labels()

Formata lista de CNPJs com nomes canonicos para mensagens de warning:

```python
def format_entity_labels(
    cnpjs: list[str],
    nomes: dict[str, str],
    limit: int = 5,
) -> str:
    """
    Se count <= limit, retorna labels separados por virgula.
    Caso contrario, retorna '{count} entidades'.

    Exemplos:
        format_entity_labels(["60872504"], {"60872504": "ITAU UNIBANCO S.A."})
        -> "60872504 (ITAU UNIBANCO S.A.)"

        format_entity_labels(["60872504"], {})
        -> "60872504"
    """
```

Extraido de `BaseExplorer` e `IFDATAExplorer` para reuso entre warning formatters.

### normalize_text()

Normaliza whitespace:

```python
def normalize_text(text: str) -> str:
    """
    Remove espacos multiplos, tabs, newlines.

    Exemplos:
        "  Banco   do    Brasil  " -> "Banco do Brasil"
        "texto\\n\\ntabs\\t\\t"    -> "texto tabs"

    Se entrada nao e string, retorna como-esta.
    """
```

---

## date.py

### normalize_date_to_int()

Converte qualquer formato de data para inteiro YYYYMM:

```python
def normalize_date_to_int(date_val: int | str | date | datetime | pd.Timestamp) -> int:
    """
    Formatos aceitos:
    - int: 202412 -> 202412
    - str: "202412" -> 202412
    - str: "2024-12" -> 202412
    - str: "2024-12-01" -> 202412
    - date: date(2024, 12, 1) -> 202412
    - datetime: datetime(2024, 12, 1) -> 202412
    - pd.Timestamp: pd.Timestamp('2024-12-01') -> 202412

    Raises:
        InvalidDateFormatError: Se formato invalido, mes fora de 1-12, ou pd.NaT/None
    """
```

### generate_month_range()

Gera lista de meses consecutivos:

```python
def generate_month_range(start: int | str | date | datetime | pd.Timestamp, end: ...) -> list[int]:
    """
    Gera meses entre start e end (inclusive).

    Exemplos:
        generate_month_range("2024-12", "2025-02")
        -> [202412, 202501, 202502]

        generate_month_range(202401, 202403)
        -> [202401, 202402, 202403]

    Se start > end, retorna lista vazia.
    """
```

### generate_quarter_range()

Gera lista de fins de trimestre:

```python
def generate_quarter_range(start: int | str | date | datetime | pd.Timestamp, end: ...) -> list[int]:
    """
    Gera fins de trimestre (03, 06, 09, 12) no range.

    Exemplos:
        generate_quarter_range(202401, 202510)
        -> [202403, 202406, 202409, 202412, 202503, 202506, 202509]

        generate_quarter_range("2024-01", "2024-06")
        -> [202403, 202406]
    """
```

### align_to_quarter_end()

Alinha YYYYMM para o fim do trimestre correspondente:

```python
def align_to_quarter_end(yyyymm: int) -> int:
    """
    Alinha para fim do trimestre (03, 06, 09, 12).

    Exemplos:
        align_to_quarter_end(202401) -> 202403
        align_to_quarter_end(202405) -> 202406
        align_to_quarter_end(202412) -> 202412
    """
```

---

## fuzzy.py

### FuzzyMatcher

Classe para busca fuzzy usando `thefuzz`:

```python
class FuzzyMatcher:
    def __init__(self, threshold_suggest: int = 78):
        """
        Args:
            threshold_suggest: Score >= para incluir em sugestoes
        """
```

### search()

Busca fuzzy retornando todos os matches acima do cutoff:

```python
def search(
    self,
    query: str,
    choices: dict[str, str],  # {identificador: label}
    score_cutoff: int = 0
) -> list[tuple[str, int]]:
    """
    Retorna: [(identificador, score), ...]

    Algoritmo:
    - Scorer: fuzz.token_set_ratio (tolerante a ordem de palavras)
    - Retorna TODOS os matches acima de score_cutoff
    - Ordenacao: score DESC, nome ASC (deterministico)
    """
```

**Exemplo**:

```python
matcher = FuzzyMatcher(threshold_suggest=78)

choices = {
    "33000167": "BANCO DO BRASIL S.A.",
    "60872504": "ITAU UNIBANCO S.A.",
    "60746948": "BANCO BRADESCO S.A.",
}

results = matcher.search("BANCO BRASIL", choices, score_cutoff=50)
# [("33000167", 95), ...]
```

---

## cnpj.py

### standardize_cnpj_base8()

Padroniza CNPJ para string de 8 digitos:

```python
def standardize_cnpj_base8(cnpj: str) -> str | None:
    """
    Remove nao-digitos, preenche com zeros, trunca para 8 chars.

    Exemplos:
        "1"                  -> "00000001"
        "33.000.167"         -> "33000167"
        "00000000000000001"  -> "00000001"
        None                 -> None
        ""                   -> None

    Algoritmo:
    1. Se None, retorna None
    2. Remove nao-digitos: re.sub(r"[^0-9]", "", ...)
    3. Se vazio, retorna None
    4. Preenche com zeros: zfill(8)
    5. Trunca: [:8]
    """
```

---

## period.py

### parse_period_from_filename()

Extrai periodo de nome de arquivo:

```python
def parse_period_from_filename(filename: str, prefix: str) -> tuple[int, int] | None:
    """
    Padroes reconhecidos:
    - {prefix}_YYYYMM: "cosif_ind_202501.parquet" -> (2025, 1)
    - {prefix}_YYYY-MM: "cosif_ind_2025-01.parquet" -> (2025, 1)

    Retorna (ano, mes) ou None se nao encontrar.
    """
```

### extract_periods_from_files()

Extrai periodos de multiplos arquivos:

```python
def extract_periods_from_files(files: list[str], prefix: str) -> list[tuple[int, int]]:
    """
    Parse cada arquivo, remove duplicatas, ordena.

    Exemplo:
        files = ["cosif_ind_202501.parquet", "cosif_ind_202502.parquet"]
        extract_periods_from_files(files, "cosif_ind")
        -> [(2025, 1), (2025, 2)]
    """
```

### get_latest_period()

Retorna periodo mais recente:

```python
def get_latest_period(files: list[str], prefix: str) -> tuple[int, int] | None:
    """
    Extrai periodos e retorna max().

    Exemplo:
        files = ["cosif_ind_202412.parquet", "cosif_ind_202501.parquet"]
        get_latest_period(files, "cosif_ind")
        -> (2025, 1)
    """
```

---

## Uso nos Modulos

### EntityLookup usa text.py e fuzzy.py

```python
from ifdata_bcb.utils import normalize_accents, FuzzyMatcher

class EntityLookup:
    def search(self, termo: str, limit: int = 10):
        # Normaliza termo para comparacao
        termo_norm = normalize_accents(termo.upper())

        # Busca fuzzy (sem limit, aplicado apos ordenar por situacao)
        matches = self._fuzzy.search(
            query=termo_norm,
            choices=nome_to_cnpj,
            score_cutoff=self._fuzzy.threshold_suggest,
        )
```

### BaseExplorer usa date.py

```python
from ifdata_bcb.utils import generate_month_range

class BaseExplorer:
    def _resolve_date_range(self, start, end, trimestral=False):
        if trimestral:
            return generate_quarter_range(start, end)
        return generate_month_range(start, end)

    # Conversao DATA int -> datetime agora feita no DuckDB via _read_glob(date_column=...)
```

### BaseCollector usa DataManager (que usa period.py internamente)

```python
class BaseCollector:
    def _get_missing_periods(self, start, end):
        # Periodos ja coletados (via DataManager)
        all_periods = self._generate_periods(start, end)
        existing = self.dm.get_periodos_disponiveis(
            self._get_file_prefix(), self._get_subdir()
        )

        # Diferenca
        existing_ints = {y * 100 + m for y, m in existing}
        return [p for p in all_periods if p not in existing_ints]
```

---

## nulls.py

### is_valid()

Check escalar de nulidade sem dependencia de pandas. Substitui `pd.notna()`/`pd.isna()` para valores individuais extraidos de DataFrames DuckDB:

```python
def is_valid(val: object) -> bool
```

Compativel com `None`, `float('nan')`, `numpy.nan`, `pd.NA` (StringDtype) e `pd.NaT`. Explora auto-desigualdade IEEE 754 (`NaN != NaN`) e trata `pd.NA` via `try/except` (comparacao ambigua).

Usado em `lookup.py`, `date.py` e `cadastro/collector.py` para checks escalares. Operacoes vetorizadas (`.notna()` sobre Series) continuam usando pandas.

---

## Exports Publicos

```python
# utils/__init__.py
from ifdata_bcb.utils.cnpj import standardize_cnpj_base8
from ifdata_bcb.utils.fuzzy import FuzzyMatcher
from ifdata_bcb.utils.text import normalize_accents, normalize_text, stem_ptbr
from ifdata_bcb.utils.date import (
    generate_month_range,
    generate_quarter_range,
    normalize_date_to_int,
)
from ifdata_bcb.utils.period import (
    parse_period_from_filename,
    extract_periods_from_files,
    get_latest_period,
)

__all__ = [
    # cnpj
    "standardize_cnpj_base8",
    # fuzzy
    "FuzzyMatcher",
    # text
    "normalize_accents",
    "normalize_text",
    "stem_ptbr",
    # date
    "generate_month_range",
    "generate_quarter_range",
    "normalize_date_to_int",
    # period
    "parse_period_from_filename",
    "extract_periods_from_files",
    "get_latest_period",
]
```

---

## Dependencias Externas

| Modulo | Dependencia | Uso |
|--------|-------------|-----|
| fuzzy.py | thefuzz | Algoritmos de fuzzy matching |
| date.py | pandas | pd.Timestamp |
| text.py | unicodedata (stdlib) | Normalizacao Unicode |
| cnpj.py | re (stdlib) | Regex |
| period.py | re (stdlib) | Regex |
