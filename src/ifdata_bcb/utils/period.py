import re


def parse_period_from_filename(filename: str, prefix: str) -> tuple[int, int] | None:
    """
    Extrai (ano, mes) de nome de arquivo com formato {prefix}_YYYYMM ou {prefix}_YYYY-MM.

    Retorna None se nao conseguir parsear.
    """
    # Formato: {prefix}_YYYYMM
    match = re.search(rf"{re.escape(prefix)}_(\d{{6}})", filename)
    if match:
        period_str = match.group(1)
        return (int(period_str[:4]), int(period_str[4:6]))

    # Formato: {prefix}_YYYY-MM
    match = re.search(rf"{re.escape(prefix)}_(\d{{4}})-(\d{{2}})", filename)
    if match:
        return (int(match.group(1)), int(match.group(2)))

    return None


def extract_periods_from_files(files: list[str], prefix: str) -> list[tuple[int, int]]:
    periods = []
    for f in files:
        period = parse_period_from_filename(f, prefix)
        if period:
            periods.append(period)
    return sorted(set(periods))


def get_latest_period(files: list[str], prefix: str) -> tuple[int, int] | None:
    periods = extract_periods_from_files(files, prefix)
    return max(periods) if periods else None
