from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from utils.local import ensure_parent_dir
from utils.s3 import read_table
from utils.dates import add_yrmo, filter_by_yrmo_retention
from utils.logging import get_logger


def run(config: Dict[str, Any]) -> pd.DataFrame:
    """Process call data and output standardized HCP call records.

    Args:
        config: Configuration dictionary containing source path, filters, and output settings.
               Expected keys: source.path, filters.product_external_id_vod__c,
               filters.months_to_retain, output.csv

    Returns:
        DataFrame with columns: HCP_ID, ACTIVITY_DATE, YRMO, ID, CHANNEL, ACTION
    """
    logger = get_logger(__name__)

    # Load source data
    source_path = config["source"]["path"]
    df = read_table(source_path)

    # Filter by product if specified
    product = config.get("filters", {}).get("product_external_id_vod__c")
    if product:
        df = df[df["product_external_id_vod__c"] == product]

    # Create copy to avoid SettingWithCopy warnings
    df = df.copy()

    # Convert call date to datetime
    df["call_date_vod__c"] = pd.to_datetime(df["call_date_vod__c"], errors="coerce")

    # Select only required columns
    df = df[
        [
            "child_account_identifier_vod__c",
            "call_date_vod__c",
            "call2_vod_id",
            "recordtype_name",
            "Action",
        ]
    ]

    # Rename columns to standard format
    df.rename(
        columns={
            "child_account_identifier_vod__c": "HCP_ID",
            "call2_vod_id": "ID",
            "recordtype_name": "CHANNEL",
            "Action": "ACTION",
            "call_date_vod__c": "ACTIVITY_DATE",
        },
        inplace=True,
    )

    # Add year-month column for filtering
    add_yrmo(df, "ACTIVITY_DATE")

    # Filter to retain only recent months
    months_to_retain = config.get("filters", {}).get("months_to_retain", 7)
    df = filter_by_yrmo_retention(df, months_to_retain)

    # Select final output columns
    df = df[["HCP_ID", "ACTIVITY_DATE", "YRMO", "ID", "CHANNEL", "ACTION"]]

    # Save to CSV
    output_path = config["output"]["csv"]
    ensure_parent_dir(output_path)
    df.to_csv(output_path, index=False)
    logger.info("Saved Call output to %s", output_path)

    return df
