# Documentacao Interna (Internals)

Esta pasta contem a documentacao tecnica interna da biblioteca `ifdata-bcb`.

## Conteudo

| Documento | Descricao |
|-----------|-----------|
| [architecture.md](architecture.md) | Visao geral da arquitetura, camadas, design patterns, fluxos |
| [core.md](core.md) | Modulo core: BaseExplorer, EntityLookup, constants |
| [domain.md](domain.md) | Modelos, tipos e hierarquia de excecoes |
| [infra.md](infra.md) | Infraestrutura: QueryEngine, DataManager, log, cache, resilience |
| [providers.md](providers.md) | Providers: BaseCollector, COSIF, IFDATA, Cadastro |
| [utils.md](utils.md) | Utilitarios: text, date, fuzzy, cnpj, period |

## Estrutura do Projeto

```
src/ifdata_bcb/
|-- __init__.py          # Entry point (lazy loading)
|-- core/                # Logica central compartilhada
|-- domain/              # Modelos e tipos
|-- providers/           # Implementacoes por fonte
|-- infra/               # Infraestrutura tecnica
|-- ui/                  # Interface visual (Display)
+-- utils/               # Utilitarios puros
```

## Ordem de Leitura Recomendada

1. **architecture.md** - Para entender a visao geral e como as camadas se conectam
2. **core.md** - Para entender a base de todos os explorers
3. **domain.md** - Para entender tipos e tratamento de erros
4. **providers.md** - Para entender como implementar novos providers
5. **infra.md** - Para entender persistencia, queries e logging
6. **utils.md** - Referencia para funcoes utilitarias

## Glossario

| Termo | Definicao |
|-------|-----------|
| **Explorer** | Classe para leitura de dados (ex: COSIFExplorer) |
| **Collector** | Classe para coleta de dados do BCB (ex: COSIFCollector) |
| **CNPJ_8** | CNPJ de 8 digitos (base, sem filial e verificadores) |
| **Escopo** | Tipo de consolidacao: individual, prudencial, financeiro |
| **Periodo** | Data no formato YYYYMM (ex: 202412) |
| **Source** | Fonte de dados (cosif_individual, ifdata_valores, etc) |

## Convencoes

- **Nomes de colunas em storage**: snake_case ou CamelCase original do BCB
- **Nomes de colunas em apresentacao**: UPPER_SNAKE_CASE padronizado
- **Datas**: Inteiro YYYYMM internamente, datetime na saida
- **CNPJs**: String de 8 digitos em todo o codigo
