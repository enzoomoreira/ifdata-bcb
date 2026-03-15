# Politica de Seguranca

## Versoes Suportadas

| Versao | Suportada |
|--------|-----------|
| 0.1.x  | Sim       |

## Reportando Vulnerabilidades

Se voce encontrar uma vulnerabilidade de seguranca, **nao abra uma issue publica**.

Use o [GitHub Security Advisories](https://github.com/enzoomoreira/ifdata-bcb/security/advisories/new) para reportar de forma privada. Inclua:

1. Descricao da vulnerabilidade
2. Passos para reproduzir
3. Impacto potencial
4. Sugestao de correcao (se tiver)

Voce recebera uma resposta em ate 7 dias uteis. Apos a correcao, voce sera creditado no release (a menos que prefira anonimato).

## Escopo

Esta biblioteca faz requisicoes HTTP para APIs publicas do Banco Central do Brasil e armazena dados localmente em cache. Os principais vetores de atencao sao:

- **Dados em cache**: arquivos parquet armazenados localmente sem criptografia
- **Requisicoes HTTP**: comunicacao com APIs do BCB (sem dados sensiveis do usuario)
- **SQL injection via DuckDB**: queries SQL construidas com input do usuario

Se encontrar algo fora desse escopo que ainda assim represente risco, reporte da mesma forma.
