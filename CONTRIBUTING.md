# Contribuindo com ifdata-bcb

Obrigado pelo interesse em contribuir! Este guia explica como participar do projeto.

## Primeiros Passos

### Requisitos

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) como gerenciador de pacotes

### Setup local

```bash
# Clone o repositorio
git clone https://github.com/enzoomoreira/ifdata-bcb.git
cd ifdata-bcb

# Instale dependencias
uv sync

# Rode os testes
uv run pytest tests/ -v
```

## Como Contribuir

### Reportando Bugs

Abra uma [issue](https://github.com/enzoomoreira/ifdata-bcb/issues) com:

- Descricao clara do problema
- Passos para reproduzir
- Comportamento esperado vs. observado
- Versao do Python e do ifdata-bcb
- Sistema operacional

### Sugerindo Features

Abra uma [issue](https://github.com/enzoomoreira/ifdata-bcb/issues) descrevendo:

- O problema que a feature resolve
- Como voce imagina a API/interface
- Exemplos de uso

### Enviando Pull Requests

1. Fork o repositorio
2. Crie uma branch a partir de `master` (`git checkout -b feat/minha-feature`)
3. Faca suas alteracoes
4. Rode os testes (`uv run pytest tests/ -v`)
5. Rode o linter (`uvx ruff check .`)
6. Commit seguindo [Conventional Commits](https://www.conventionalcommits.org/):
   - `feat:` nova funcionalidade
   - `fix:` correcao de bug
   - `docs:` documentacao
   - `refactor:` refatoracao sem mudanca de comportamento
   - `test:` adicao ou correcao de testes
7. Abra o PR contra `master`

## Estilo de Codigo

- **Formatacao**: ruff format
- **Linting**: ruff check
- **Type hints**: obrigatorios em todas as funcoes
- **Docstrings**: apenas em API publica ou comportamento nao-obvio
- **Encoding**: UTF-8, sem emojis

## Estrutura do Projeto

```
src/ifdata_bcb/
  core/        # Logica central (BaseExplorer, EntityLookup, Constants)
  domain/      # Modelos, excecoes, validacao
  infra/       # Infraestrutura (paths, config, QueryEngine)
  providers/   # Provedores de dados (cosif, ifdata)
  utils/       # Utilitarios
  ui/          # Componentes de interface
```

## Testes

```bash
# Rodar todos os testes
uv run pytest tests/ -v

# Rodar um modulo especifico
uv run pytest tests/test_validation.py -v
```

Os testes usam dados reais do BCB. Algumas execucoes podem ser lentas na primeira vez por conta da coleta de dados.

## Duvidas?

Abra uma [issue](https://github.com/enzoomoreira/ifdata-bcb/issues) com sua pergunta. Ficaremos felizes em ajudar.
