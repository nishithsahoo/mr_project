from __future__ import annotations

from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd


def to_datetime(series: pd.Series, errors: str = "coerce") -> pd.Series:
    """Convert a pandas Series to datetime.

    Args:
        series: Input series to convert
        errors: How to handle errors ('coerce', 'raise', 'ignore')

    Returns:
        Series converted to datetime type
    """
    return pd.to_datetime(series, errors=errors)


def add_yrmo(df: pd.DataFrame, date_col: str, yrmo_col: str = "YRMO") -> pd.DataFrame:
    """Add a year-month column (YRMO) to a dataframe based on a date column.

    Args:
        df: Input dataframe to modify
        date_col: Name of the column containing dates
        yrmo_col: Name of the output column (default: 'YRMO')

    Returns:
        Modified dataframe with YRMO column added
    """
    df[yrmo_col] = (
        pd.to_datetime(df[date_col], errors="coerce").dt.to_period("M").astype(str)
    )
    return df


def filter_by_yrmo_retention(
    df: pd.DataFrame, months_to_retain: int = 7, yrmo_col: str = "YRMO"
) -> pd.DataFrame:
    """Filter dataframe to keep only the last N months based on YRMO column.

    Finds the maximum YRMO value, calculates the start date N months before,
    and filters the dataframe to keep only rows within this retention period.

    Args:
        df: Input dataframe to filter
        months_to_retain: Number of months to retain (default: 7)
        yrmo_col: Name of the YRMO column (default: 'YRMO')

    Returns:
        Filtered dataframe containing only recent N months of data
    """
    if df.empty or yrmo_col not in df.columns:
        return df

    max_yrmo = df[yrmo_col].max()
    if pd.isna(max_yrmo):
        return df

    max_date = datetime.strptime(max_yrmo, "%Y-%m")
    start_date = max_date - relativedelta(months=months_to_retain - 1)
    start_yrmo = start_date.strftime("%Y-%m")

    return df[df[yrmo_col] >= start_yrmo].copy()
