"""Testes de warnings estruturados com atributos programaticos.

Verifica que warnings carregam dados acessiveis via warning.message.attr,
permitindo captura programatica em backends e agents.
"""

import warnings


from ifdata_bcb.domain.exceptions import (
    EmptyFilterWarning,
    IncompatibleEraWarning,
    NullValuesWarning,
    PartialDataWarning,
    ScopeUnavailableWarning,
)
from ifdata_bcb.infra.log import emit_user_warning


class TestWarningAttributes:
    """Cada warning class expoe atributos estruturados."""

    def test_incompatible_era_has_boundary_and_source(self) -> None:
        w = IncompatibleEraWarning("msg", boundary=202503, source="IFDATA")
        assert w.boundary == 202503
        assert w.source == "IFDATA"
        assert str(w) == "msg"

    def test_partial_data_has_reason_and_detail(self) -> None:
        w = PartialDataWarning("msg", reason="no_files", detail={"key": "val"})
        assert w.reason == "no_files"
        assert w.detail == {"key": "val"}

    def test_partial_data_defaults(self) -> None:
        w = PartialDataWarning("msg")
        assert w.reason == ""
        assert w.detail is None

    def test_scope_unavailable_has_entities_escopo_periodos(self) -> None:
        w = ScopeUnavailableWarning(
            "msg",
            entities=["60872504", "90400888"],
            escopo="prudencial",
            periodos=[202303, 202306],
        )
        assert w.entities == ["60872504", "90400888"]
        assert w.escopo == "prudencial"
        assert w.periodos == [202303, 202306]

    def test_null_values_has_entities(self) -> None:
        w = NullValuesWarning("msg", entities=["60872504"])
        assert w.entities == ["60872504"]

    def test_empty_filter_has_parameter(self) -> None:
        w = EmptyFilterWarning("msg", parameter="columns")
        assert w.parameter == "columns"


class TestEmitUserWarningDualMode:
    """emit_user_warning aceita str (legacy) e Warning (novo)."""

    def test_emit_string_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            emit_user_warning("mensagem teste", PartialDataWarning, stacklevel=1)
        assert len(w) == 1
        assert issubclass(w[0].category, PartialDataWarning)
        assert "mensagem teste" in str(w[0].message)

    def test_emit_instance_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            warning_obj = PartialDataWarning("msg estruturado", reason="test")
            emit_user_warning(warning_obj, stacklevel=1)
        assert len(w) == 1
        assert issubclass(w[0].category, PartialDataWarning)
        assert w[0].message.reason == "test"

    def test_emit_instance_preserves_type(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            emit_user_warning(
                NullValuesWarning("null!", entities=["123"]),
                stacklevel=1,
            )
        assert issubclass(w[0].category, NullValuesWarning)
        assert w[0].message.entities == ["123"]


class TestWarningCapturePattern:
    """Simula o padrao de captura que backends/agents usariam."""

    def test_capture_structured_data_from_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            emit_user_warning(
                ScopeUnavailableWarning(
                    "Escopo prudencial indisponivel",
                    entities=["60872504", "90400888"],
                    escopo="prudencial",
                    periodos=[202303],
                ),
                stacklevel=1,
            )

        assert len(w) == 1
        msg = w[0].message
        assert hasattr(msg, "entities")
        assert hasattr(msg, "escopo")
        assert hasattr(msg, "periodos")
        assert len(msg.entities) == 2
        assert msg.escopo == "prudencial"
