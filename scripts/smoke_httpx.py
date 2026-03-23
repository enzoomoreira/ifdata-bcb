"""Smoke test: verifica que httpx funciona como drop-in do requests."""

import httpx

from ifdata_bcb.infra.resilience import TRANSIENT_EXCEPTIONS, retry


def test_transient_exceptions_no_requests_refs() -> None:
    print("[1] Verificando TRANSIENT_EXCEPTIONS...")
    for exc in TRANSIENT_EXCEPTIONS:
        module = exc.__module__
        assert "requests" not in module, f"Ainda referencia requests: {exc}"
        assert "urllib3" not in module, f"Ainda referencia urllib3: {exc}"
    print("    OK: nenhuma referencia a requests/urllib3")


def test_httpx_get_basic() -> None:
    print("[2] Testando httpx.get() basico...")
    r = httpx.get("https://httpbin.org/get", timeout=10, follow_redirects=True)
    assert r.status_code == 200
    print(f"    OK: status {r.status_code}")


def test_retry_with_httpx_exceptions() -> None:
    print("[3] Testando retry com httpx.ConnectError...")
    counter = {"n": 0}

    @retry(max_attempts=2, delay=0.1, jitter=False)
    def fail_then_ok() -> str:
        counter["n"] += 1
        if counter["n"] == 1:
            raise httpx.ConnectError("test")
        return "ok"

    assert fail_then_ok() == "ok"
    print("    OK: retry funciona com httpx exceptions")


def test_client_connection_pooling() -> None:
    print("[4] Testando httpx.Client (connection pooling)...")
    with httpx.Client(timeout=10, follow_redirects=True) as client:
        r1 = client.get("https://httpbin.org/get")
        r2 = client.get("https://httpbin.org/get")
        assert r1.status_code == 200
        assert r2.status_code == 200
    print("    OK: Client com pooling funciona")


def test_raise_for_status() -> None:
    print("[5] Testando raise_for_status (404)...")
    r = httpx.get("https://httpbin.org/status/404", timeout=10, follow_redirects=True)
    assert r.status_code == 404
    try:
        r.raise_for_status()
        raise AssertionError("Deveria ter levantado excecao")
    except httpx.HTTPStatusError:
        pass
    print("    OK: raise_for_status levanta HTTPStatusError")


def test_bcb_cosif_endpoint() -> None:
    print("[6] Testando endpoint real BCB (COSIF)...")
    url = "https://www.bcb.gov.br/content/estabilidadefinanceira/cosif/Bancos/202412BANCOS.csv.zip"
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        r = client.get(url)
        assert r.status_code == 200
        assert len(r.content) > 1000
    print(f"    OK: BCB COSIF respondeu {r.status_code}, {len(r.content):,} bytes")


def test_bcb_ifdata_endpoint() -> None:
    print("[7] Testando endpoint real BCB (IFDATA Cadastro)...")
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"
        "/IfDataCadastro(AnoMes=@AnoMes)?@AnoMes=202412&$format=text/csv"
    )
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        r = client.get(url)
        assert r.status_code == 200
        assert len(r.text) > 100
    print(f"    OK: BCB IFDATA respondeu {r.status_code}, {len(r.text):,} chars")


def test_no_requests_imports_in_codebase() -> None:
    print("[8] Verificando imports no codebase...")
    from pathlib import Path

    src_dir = Path(__file__).parent.parent / "src"
    violations = []
    for py_file in src_dir.rglob("*.py"):
        content = py_file.read_text(encoding="utf-8")
        for i, line in enumerate(content.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if "import requests" in stripped or "import urllib3" in stripped:
                violations.append(
                    f"  {py_file.relative_to(src_dir.parent)}:{i}: {stripped}"
                )

    if violations:
        print("    FALHA: imports residuais encontrados:")
        for v in violations:
            print(v)
        raise AssertionError(f"{len(violations)} import(s) residual(is)")
    print("    OK: nenhum import de requests/urllib3 no src/")


if __name__ == "__main__":
    test_transient_exceptions_no_requests_refs()
    test_httpx_get_basic()
    test_retry_with_httpx_exceptions()
    test_client_connection_pooling()
    test_raise_for_status()
    test_bcb_cosif_endpoint()
    test_bcb_ifdata_endpoint()
    test_no_requests_imports_in_codebase()
    print("\n=== Todos os smoke tests passaram ===")
