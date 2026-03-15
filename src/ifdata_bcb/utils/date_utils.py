"""
Utilities for date manipulation and range generation.
"""

from datetime import datetime, date
from typing import List, Union

import pandas as pd

# Type alias for flexible date input
DateInput = Union[str, int]

def _parse_date_input(date_input: DateInput) -> date:
    """
    Internal helper to convert input to a datetime.date object.
    Handles strings ('YYYY-MM-DD', 'YYYYMM') and integers (YYYYMM).
    """
    if isinstance(date_input, int):
        year, month = divmod(date_input, 100)
        # Default to first day of month for integer inputs
        return date(year, month, 1)
    
    if isinstance(date_input, str):
        # Clean input string
        clean_date = date_input.strip()
        
        # Try YYYYMM format
        if len(clean_date) == 6 and clean_date.isdigit():
            return date(int(clean_date[:4]), int(clean_date[4:]), 1)
            
        # Try YYYY-MM-DD format
        try:
            return datetime.strptime(clean_date, '%Y-%m-%d').date()
        except ValueError:
            # Try YYYY-MM format
            try:
                return datetime.strptime(clean_date, '%Y-%m').date()
            except ValueError:
                pass
                
    raise ValueError(f"Formato de data inválido: {date_input}")

def normalize_date_to_int(date_val: DateInput) -> int:
    """
    Converts a date in various formats to integer YYYYMM format.

    Accepts:
        - int: 202412 (passthrough with validation)
        - str: '2024-12-31', '2024-12', '202412'

    Returns:
        Integer in YYYYMM format (e.g., 202412)

    Raises:
        ValueError: If date format is invalid or date is out of reasonable range
    """
    if isinstance(date_val, int):
        # Validate integer range (190000 - 210012)
        if not (190000 <= date_val <= 210012):
            raise ValueError(f"Data fora do intervalo válido (190001-210012): {date_val}")
        
        # Validate month logic
        month = date_val % 100
        if not (1 <= month <= 12):
            raise ValueError(f"Mês inválido em {date_val}: {month}")
        return date_val

    # For string inputs, parse and convert to integer
    try:
        parsed_date = _parse_date_input(date_val)
        return parsed_date.year * 100 + parsed_date.month
    except Exception as e:
        raise ValueError(f"Erro ao processar data string: {date_val}. Detalhes: {e}")

def generate_month_range(start: DateInput, end: DateInput) -> List[int]:
    """
    Generates a list of months in YYYYMM format between start and end (inclusive).

    Returns:
        List of integers in YYYYMM format.
    """
    start_int = normalize_date_to_int(start)
    end_int = normalize_date_to_int(end)

    if start_int > end_int:
        return []

    months = []
    current = start_int
    
    # Extract initial year and month
    curr_year, curr_month = divmod(current, 100)
    
    # Extract target year and month
    end_year, end_month = divmod(end_int, 100)

    # Calculate total number of months to iterate
    total_months = (end_year - curr_year) * 12 + (end_month - curr_month) + 1

    # Iterate generating sequential months
    for _ in range(total_months):
        months.append(curr_year * 100 + curr_month)
        
        curr_month += 1
        if curr_month > 12:
            curr_month = 1
            curr_year += 1

    return months

def generate_quarter_range(start: DateInput, end: DateInput) -> List[int]:
    """
    Generates a list of quarter-ending months in YYYYMM format between start and end.
    
    Logic ensures that if a date falls within a quarter, the end of that quarter 
    is calculated as the reference point.

    Returns:
        List of integers in YYYYMM format corresponding to quarter ends (03, 06, 09, 12).
    """
    start_int = normalize_date_to_int(start)
    end_int = normalize_date_to_int(end)

    if start_int > end_int:
        return []

    quarters = []
    
    # Parse start to determine the first quarter end
    s_year, s_month = divmod(start_int, 100)
    
    # Calculate which quarter the start date belongs to (1-4)
    # Q1: Months 1-3, Q2: 4-6, etc.
    curr_quarter_idx = (s_month - 1) // 3 + 1
    
    # Calculate the last month of that quarter (3, 6, 9, or 12)
    curr_q_month = curr_quarter_idx * 3
    curr_year = s_year
    
    # Construct the first quarter-end date in YYYYMM integer format
    current_q_date = curr_year * 100 + curr_q_month

    # Iterate jumping 3 months at a time
    while current_q_date <= end_int:
        quarters.append(current_q_date)
        
        # Move to next quarter
        curr_q_month += 3
        if curr_q_month > 12:
            curr_q_month = 3
            curr_year += 1
            
        current_q_date = curr_year * 100 + curr_q_month

    return quarters


def yyyymm_to_datetime(value: int) -> pd.Timestamp:
    """
    Converte YYYYMM (int) para pandas Timestamp (primeiro dia do mes).

    Args:
        value: Inteiro no formato YYYYMM (ex: 202412).

    Returns:
        pd.Timestamp correspondente ao primeiro dia do mes.

    Exemplo:
        >>> yyyymm_to_datetime(202412)
        Timestamp('2024-12-01')
    """
    year, month = divmod(int(value), 100)
    return pd.Timestamp(year=year, month=month, day=1)