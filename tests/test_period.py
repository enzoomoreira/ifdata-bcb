"""Testes para ifdata_bcb.utils.period."""


from ifdata_bcb.utils.period import (
    extract_periods_from_files,
    get_latest_period,
    parse_period_from_filename,
)


class TestParsePeriodFromFilename:
    """parse_period_from_filename: extrai (ano, mes) de nomes de arquivo."""

    def test_yyyymm_format(self) -> None:
        assert parse_period_from_filename("ifdata_cad_202412", "ifdata_cad") == (
            2024,
            12,
        )

    def test_yyyy_mm_format(self) -> None:
        assert parse_period_from_filename("ifdata_cad_2024-03", "ifdata_cad") == (
            2024,
            3,
        )

    def test_no_match_returns_none(self) -> None:
        assert parse_period_from_filename("random_file", "ifdata_cad") is None

    def test_wrong_prefix_returns_none(self) -> None:
        assert parse_period_from_filename("cosif_ind_202412", "ifdata_cad") is None

    def test_prefix_with_special_chars(self) -> None:
        # re.escape deve tratar prefixos com caracteres regex
        assert parse_period_from_filename("data.file_202301", "data.file") == (2023, 1)

    def test_month_zero_parsed_without_validation(self) -> None:
        # A funcao apenas faz parsing, nao valida intervalo de mes
        assert parse_period_from_filename("prefix_202400", "prefix") == (2024, 0)

    def test_month_13_parsed_without_validation(self) -> None:
        assert parse_period_from_filename("prefix_202413", "prefix") == (2024, 13)

    def test_embedded_in_longer_filename(self) -> None:
        # O pattern usa re.search, deve encontrar dentro de nomes maiores
        assert parse_period_from_filename(
            "dir/ifdata_cad_202412.parquet", "ifdata_cad"
        ) == (
            2024,
            12,
        )

    def test_empty_filename(self) -> None:
        assert parse_period_from_filename("", "prefix") is None

    def test_empty_prefix(self) -> None:
        # Prefix vazio: "_202412" deve funcionar pois re.escape("") = ""
        assert parse_period_from_filename("_202412", "") == (2024, 12)


class TestExtractPeriodsFromFiles:
    """extract_periods_from_files: extrai e ordena periodos de lista de arquivos."""

    def test_multiple_files(self) -> None:
        files = ["ifdata_cad_202412", "ifdata_cad_202401", "ifdata_cad_202406"]
        result = extract_periods_from_files(files, "ifdata_cad")
        assert result == [(2024, 1), (2024, 6), (2024, 12)]

    def test_deduplicates(self) -> None:
        files = ["p_202401", "p_202401", "p_202402"]
        result = extract_periods_from_files(files, "p")
        assert result == [(2024, 1), (2024, 2)]

    def test_empty_list(self) -> None:
        assert extract_periods_from_files([], "p") == []

    def test_no_valid_files(self) -> None:
        assert extract_periods_from_files(["garbage", "junk"], "p") == []

    def test_mixed_valid_invalid(self) -> None:
        files = ["p_202301", "nope", "p_202312"]
        result = extract_periods_from_files(files, "p")
        assert result == [(2023, 1), (2023, 12)]

    def test_sorted_across_years(self) -> None:
        files = ["p_202501", "p_202312", "p_202401"]
        result = extract_periods_from_files(files, "p")
        assert result == [(2023, 12), (2024, 1), (2025, 1)]


class TestGetLatestPeriod:
    """get_latest_period: retorna periodo mais recente."""

    def test_returns_latest(self) -> None:
        files = ["p_202301", "p_202412", "p_202406"]
        assert get_latest_period(files, "p") == (2024, 12)

    def test_empty_returns_none(self) -> None:
        assert get_latest_period([], "p") is None

    def test_no_valid_returns_none(self) -> None:
        assert get_latest_period(["garbage"], "p") is None

    def test_single_file(self) -> None:
        assert get_latest_period(["p_202501"], "p") == (2025, 1)
