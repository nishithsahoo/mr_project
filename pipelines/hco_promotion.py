from __future__ import annotations

from typing import Any, Dict

import numpy as np
import pandas as pd

from utils.local import ensure_parent_dir
from utils.s3 import read_table
from utils.logging import get_logger


def run(config: Dict[str, Any]) -> pd.DataFrame:
    """Concatenate processed pipeline outputs into a single HCP dataset.

    Args:
        config: Configuration dictionary containing source paths and output settings.
               Expected keys: sources.event.path, sources.edetail.path,
               sources.call.path, sources.vae.path, output.csv

    Returns:
        DataFrame containing all concatenated HCP records
    """
    logger = get_logger(__name__)

    # Get source file paths
    sources = config.get("sources", {})

    # Load all processed pipeline outputs
    event = read_table(sources["event"]["path"])
    edetail = read_table(sources["edetail"]["path"])
    call = read_table(sources["call"]["path"])
    vae = read_table(sources["vae"]["path"])

    # Concatenate all pipeline outputs
    hcp = pd.concat([event, edetail, call, vae], ignore_index=True)

    # Save concatenated output
    output_path = config["output"]["csv"]
    ensure_parent_dir(output_path)
    hcp.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Saved concatenated HCP output to %s", output_path)

    return hcp
