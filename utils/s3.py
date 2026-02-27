import pandas as pd


def read_parquet(path: str) -> pd.DataFrame:
    """Read a parquet file into a pandas DataFrame.

    Args:
        path: File path to the parquet file

    Returns:
        DataFrame containing the parquet file data
    """
    return pd.read_parquet(path)


def read_csv(path: str) -> pd.DataFrame:
    """Read a CSV file into a pandas DataFrame.

    Args:
        path: File path to the CSV file

    Returns:
        DataFrame containing the CSV file data
    """
    return pd.read_csv(path)


def read_table(path: str) -> pd.DataFrame:
    """Read a table file (CSV or Parquet) into a pandas DataFrame.

    Automatically detects file type based on extension and uses
    appropriate reader function.

    Args:
        path: File path to the table file (.csv or .parquet)

    Returns:
        DataFrame containing the file data
    """
    if path.lower().endswith(".csv"):
        return read_csv(path)
    return read_parquet(path)
