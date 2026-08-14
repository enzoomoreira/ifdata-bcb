"""Testes para ifdata_bcb.domain.exceptions."""

import subprocess
import sys
import warnings
from pathlib import Path

import pytest

import ifdata_bcb as bcb
from ifdata_bcb.domain import exceptions
from ifdata_bcb.domain.exceptions import (
    BacenAnalysisError,
    BacenWarning,
    DataProcessingError,
    DataUnavailableError,
    DroppedReportWarning,
    EmptyFilterWarning,
    IncompatibleEraWarning,
    InvalidColumnError,
    InvalidDateFormatError,
    InvalidDateRangeError,
    InvalidIdentifierError,
    InvalidScopeError,
    MissingRequiredParameterError,
    NullValuesWarning,
    PartialDataWarning,
    PeriodUnavailableError,
    ScopeMigrationWarning,
    ScopeUnavailableWarning,
    TruncatedResultWarning,
)


class TestExceptionHierarchy:
    """Todas as exceptions devem herdar de BacenAnalysisError."""

    @pytest.mark.parametrize(
        "exc_class",
        [
            InvalidScopeError,
            DataUnavailableError,
            InvalidIdentifierError,
            MissingRequiredParameterError,
            InvalidDateRangeError,
            InvalidDateFormatError,
            PeriodUnavailableError,
            DataProcessingError,
            InvalidColumnError,
        ],
    )
    def test_inherits_from_base(self, exc_class: type) -> None:
        assert issubclass(exc_class, BacenAnalysisError)


class TestWarningHierarchy:
    """Todos os warnings devem herdar de BacenWarning (e continuar UserWarning)."""

    WARNINGS = (
        DroppedReportWarning,
        EmptyFilterWarning,
        IncompatibleEraWarning,
        NullValuesWarning,
        PartialDataWarning,
        ScopeMigrationWarning,
        ScopeUnavailableWarning,
        TruncatedResultWarning,
    )

    @pytest.mark.parametrize("warn_class", WARNINGS)
    def test_inherits_from_bacen_warning(self, warn_class: type) -> None:
        assert issubclass(warn_class, BacenWarning)

    @pytest.mark.parametrize("warn_class", WARNINGS)
    def test_still_a_user_warning(self, warn_class: type) -> None:
        """Quem ja filtrava por UserWarning nao pode ser afetado."""
        assert issubclass(warn_class, UserWarning)

    def test_um_filtro_silencia_todos(self) -> None:
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter("always")
            warnings.simplefilter("ignore", BacenWarning)
            warnings.warn(PartialDataWarning("da lib"), stacklevel=1)
            warnings.warn(TruncatedResultWarning("da lib", limit=1), stacklevel=1)
            warnings.warn(UserWarning("de outra lib"), stacklevel=1)
        assert [str(w.message) for w in rec] == ["de outra lib"]


class TestTopLevelReexport:
    """3.8: tratar erro nao pode exigir conhecer ifdata_bcb.domain.exceptions."""

    def test_todo_nome_de_all_e_alcancavel(self) -> None:
        inacessiveis = [n for n in bcb.__all__ if not hasattr(bcb, n)]
        assert inacessiveis == []

    @pytest.mark.parametrize(
        "nome",
        [
            "BacenAnalysisError",
            "BacenWarning",
            "InvalidColumnError",
            "InvalidScopeError",
            "PartialDataWarning",
            "TruncatedResultWarning",
        ],
    )
    def test_e_a_mesma_classe_do_modulo_interno(self, nome: str) -> None:
        assert getattr(bcb, nome) is getattr(exceptions, nome)

    def test_toda_excecao_exportada_e_levantada_em_algum_lugar(self) -> None:
        """3.9: DataUnavailableError ficou anos exportada sem nenhum `raise`.

        Quem escrevia `except DataUnavailableError` tinha handler morto e nao
        tinha como descobrir. Este teste faz o contrato falhar alto: se um
        nome esta no __all__ como erro, algum caminho do src/ tem que levanta-lo.
        """
        src = Path(exceptions.__file__).parent.parent
        codigo = "\n".join(p.read_text(encoding="utf-8") for p in src.rglob("*.py"))
        exportadas = [
            n
            for n in bcb.__all__
            if isinstance(getattr(bcb, n, None), type)
            and issubclass(getattr(bcb, n), BacenAnalysisError)
            and n != "BacenAnalysisError"  # base, capturada e nao levantada
        ]
        assert exportadas, "nenhuma excecao exportada -- o filtro quebrou"
        sem_raise = [n for n in exportadas if f"raise {n}(" not in codigo]
        assert sem_raise == []

    def test_reexport_nao_carrega_pandas(self) -> None:
        """As excecoes sao eager; o lazy loading dos explorers tem que sobreviver.

        Subprocesso porque no processo do pytest o pandas ja foi importado por
        outros testes -- aqui so um interpretador limpo responde a pergunta.
        """
        saida = subprocess.run(
            [
                sys.executable,
                "-c",
                "import ifdata_bcb, sys; print('pandas' in sys.modules)",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
        )
        assert saida.stdout.strip() == "False"


class TestInvalidScopeError:
    def test_message_contains_valid_values(self) -> None:
        err = InvalidScopeError("escopo", "xyz", ["individual", "prudencial"])
        assert "xyz" in str(err)
        assert "individual" in str(err)
        assert "prudencial" in str(err)

    def test_attributes(self) -> None:
        err = InvalidScopeError("escopo", "xyz", ["a", "b"])
        assert err.scope == "escopo"
        assert err.value == "xyz"
        assert err.valid_values == ["a", "b"]

    def test_message_names_the_parameter(self) -> None:
        """Nao pode dizer 'Escopo' para documento/fonte/source."""
        err = InvalidScopeError("documento", "abc", [])
        assert "'documento'" in str(err)
        assert "Escopo" not in str(err)

    def test_valid_values_vazio_omite_a_clausula(self) -> None:
        err = InvalidScopeError("documento", "abc", [])
        assert "Validos" not in str(err)
        assert err.valid_values == []

    def test_hint_e_anexado(self) -> None:
        err = InvalidScopeError("documento", "abc", [], hint="Use list().")
        assert str(err).endswith("Use list().")
        assert err.hint == "Use list()."

    def test_valid_values_str_nao_e_quebrado_em_caracteres(self) -> None:
        """Regressao: passar str onde se espera list gerava 'v', 'a', 'l', ...

        O bug real estava no call site, mas o sintoma so aparecia aqui.
        """
        err = InvalidScopeError("documento", "abc", ["4010", "4016"])
        assert "'4010', '4016'" in str(err)


class TestDataUnavailableError:
    def test_message_with_reason(self) -> None:
        err = DataUnavailableError("12345678", "prudencial", "Sem conglomerado.")
        assert "12345678" in str(err)
        assert "prudencial" in str(err)
        assert "Sem conglomerado." in str(err)

    def test_message_without_reason(self) -> None:
        err = DataUnavailableError("12345678", "cosif")
        assert "12345678" in str(err)
        assert "cosif" in str(err)


class TestInvalidDateFormatError:
    def test_with_detail(self) -> None:
        err = InvalidDateFormatError("abc", "mes invalido")
        assert "abc" in str(err)
        assert "mes invalido" in str(err)

    def test_without_detail(self) -> None:
        err = InvalidDateFormatError("abc")
        assert "abc" in str(err)
