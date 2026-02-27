from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from utils.local import ensure_parent_dir
from utils.s3 import read_table
from utils.dates import add_yrmo, filter_by_yrmo_retention
from utils.logging import get_logger


def run(config: Dict[str, Any]) -> pd.DataFrame:
    """Process VAE (LMMR) data and output standardized HCP LMMR records.

    Args:
        config: Configuration dictionary containing source path, filters, and output settings.
               Expected keys: source.path, filters.product_id,
               filters.months_to_retain, output.csv

    Returns:
        DataFrame with columns: HCP_ID, ACTIVITY_DATE, YRMO, ID, CHANNEL, ACTION
    """
    logger = get_logger(__name__)

    # Load source data
    df = read_table(config["source"]["path"])

    # Filter by product if specified
    product_id = config.get("filters", {}).get("product_id")
    if product_id:
        df = df[df["product_id"] == product_id]

    # Create copy to avoid warnings
    df = df.copy()

    # Convert activity date and sort records
    df["yrmoda"] = pd.to_datetime(df["activity_date"], errors="coerce")
    df.sort_values(
        by=["customer_id", "yrmoda", "sevc_id", "action"],
        ascending=True,
        inplace=True,
    )

    # Set channel to LMMR
    df["CHANNEL"] = "LMMR"

    # Rename columns to standard format
    df.rename(
        columns={
            "customer_id": "HCP_ID",
            "activity_date": "ACTIVITY_DATE",
            "sevc_id": "ID",
            "action": "ACTION",
        },
        inplace=True,
    )

    add_yrmo(df, "ACTIVITY_DATE")

    months_to_retain = config.get("filters", {}).get("months_to_retain", 7)
    df = filter_by_yrmo_retention(df, months_to_retain)

    df = df[["HCP_ID", "ACTIVITY_DATE", "YRMO", "ID", "CHANNEL", "ACTION"]]

    output_path = config["output"]["csv"]
    ensure_parent_dir(output_path)
    df.to_csv(output_path, index=False)
    logger.info("Saved VAE output to %s", output_path)

    return df
