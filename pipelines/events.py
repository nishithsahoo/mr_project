from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from utils.local import ensure_parent_dir
from utils.s3 import read_table
from utils.dates import add_yrmo, filter_by_yrmo_retention
from utils.logging import get_logger


def run(config: Dict[str, Any]) -> pd.DataFrame:
    """Process event/conference data and output standardized HCP event records.

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

    # Remove rows with missing channel and reset index
    df = df.dropna(subset=["channel"]).reset_index(drop=True)
    # Convert activity start date to datetime
    df["ACTVY_STRT_DT"] = pd.to_datetime(df["ACTVY_STRT_DT"], errors="coerce")

    # Filter by product if specified
    product_id = config.get("filters", {}).get("product_id")
    if product_id:
        df = df[df["product_id"].astype(str) == str(product_id)]

    # Rename columns to standard format
    df.rename(
        columns={
            "conference_id": "ID",
            "customer_id": "HCP_ID",
            "product_id": "APIMS_ID",
            "indication_id": "INDCTN_ID",
            "channel": "CHANNEL",
            "action": "ACTION",
            "ACTVY_STRT_DT": "ACTIVITY_DATE",
        },
        inplace=True,
    )

    add_yrmo(df, "ACTIVITY_DATE")

    months_to_retain = config.get("filters", {}).get("months_to_retain", 7)
    df = filter_by_yrmo_retention(df, months_to_retain)

    new_order = ["HCP_ID", "ACTIVITY_DATE", "YRMO", "ID", "CHANNEL", "ACTION"]
    df = df[new_order]

    output_path = config["output"]["csv"]
    ensure_parent_dir(output_path)
    df.to_csv(output_path, index=False)
    logger.info("Saved Events output to %s", output_path)

    return df
