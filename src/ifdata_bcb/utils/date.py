from calendar import monthrange
from datetime import date, datetime

import pandas as pd

from ifdata_bcb.domain.exceptions import InvalidDateFormatError


def _parse_date_input(date_input: int | str) -> date:
    # Aceita: int YYYYMM, str 'YYYYMM', 'YYYY-MM', 'YYYY-MM-DD'
    if isinstance(date_input, int):
        year, month = divmod(date_input, 100)
        return date(year, month, 1)

    if isinstance(date_input, str):
        clean_date = date_input.strip()

        if len(clean_date) == 6 and clean_date.isdigit():
            return date(int(clean_date[:4]), int(clean_date[4:]), 1)

        try:
            return datetime.strptime(clean_date, "%Y-%m-%d").date()
        except ValueError:
            try:
                return datetime.strptime(clean_date, "%Y-%m").date()
            except ValueError:
                pass

    raise InvalidDateFormatError(str(date_input))


def normalize_date_to_int(date_val: int | str) -> int:
    """Converte data para int YYYYMM. Aceita int ou str ('YYYYMM', 'YYYY-MM', 'YYYY-MM-DD')."""
    if isinstance(date_val, int):
        month = date_val % 100
        if not (1 <= month <= 12):
            raise InvalidDateFormatError(str(date_val), f"mes invalido: {month}")
        return date_val

    parsed_date = _parse_date_input(date_val)
    return parsed_date.year * 100 + parsed_date.month


def generate_month_range(start: int | str, end: int | str) -> list[int]:
    """Gera lista de meses YYYYMM entre start e end (inclusive)."""
    start_int = normalize_date_to_int(start)
    end_int = normalize_date_to_int(end)

    if start_int > end_int:
        return []

    months = []
    curr_year, curr_month = divmod(start_int, 100)
    end_year, end_month = divmod(end_int, 100)

    total_months = (end_year - curr_year) * 12 + (end_month - curr_month) + 1

    for _ in range(total_months):
        months.append(curr_year * 100 + curr_month)

        curr_month += 1
        if curr_month > 12:
            curr_month = 1
            curr_year += 1

    return months


def generate_quarter_range(start: int | str, end: int | str) -> list[int]:
    """Gera lista de fins de trimestre YYYYMM (03, 06, 09, 12) entre start e end."""
    start_int = normalize_date_to_int(start)
    end_int = normalize_date_to_int(end)

    if start_int > end_int:
        return []

    quarters = []

    s_year, s_month = divmod(start_int, 100)
    curr_quarter_idx = (s_month - 1) // 3 + 1
    curr_q_month = curr_quarter_idx * 3
    curr_year = s_year

    current_q_date = curr_year * 100 + curr_q_month

    while current_q_date <= end_int:
        quarters.append(current_q_date)

        curr_q_month += 3
        if curr_q_month > 12:
            curr_q_month = 3
            curr_year += 1

        current_q_date = curr_year * 100 + curr_q_month

    return quarters


def align_to_quarter_end(yyyymm: int) -> int:
    """Alinha YYYYMM para o fim do trimestre correspondente (03, 06, 09, 12)."""
    year, month = divmod(yyyymm, 100)
    quarter_month = ((month - 1) // 3 + 1) * 3
    return year * 100 + quarter_month


def yyyymm_to_datetime(value: int) -> pd.Timestamp:
    year, month = divmod(int(value), 100)
    last_day = monthrange(year, month)[1]
    return pd.Timestamp(year=year, month=month, day=last_day)
