from __future__ import annotations

from typing import Any, Dict

import numpy as np
import pandas as pd

from utils.local import ensure_parent_dir
from utils.s3 import read_table
from utils.dates import add_yrmo, filter_by_yrmo_retention
from utils.logging import get_logger


CHANNEL_MAPPING = {
    "M3": "EMAIL_M3_MR_KUN",
    "M3-Quiz": "EMAIL_M3_QUIZ",
    "M3-MM": "EMAIL_M3_MM",
    "NMO": "EDETAIL_NMO",
    "M3-OPD": "EMAIL_M3_OPD",
    "CARENET": "EDETAIL_CARENET",
    "JSTREAM": "EDETAIL_JSTREAM",
    "Medpeer": "EDETAIL_MEDPEER",
}


def _normalize_edetail(df: pd.DataFrame, product_name: str) -> pd.DataFrame:
    """Normalize edetail data by standardizing columns and filtering by product.

    Args:
        df: Raw edetail dataframe
        product_name: Product name to filter on

    Returns:
        Normalized dataframe with standardized column names
    """
    df = df.copy()
    df["activity_date"] = pd.to_datetime(
        df["activity_date"],
        format="mixed",
        dayfirst=False,
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")

    df = df[
        [
            "src_systm_cd",
            "dgtl_dtl_only_id",
            "action",
            "activity_date",
            "customer_id",
            "product_indication_id",
            "product_name",
        ]
    ]

    df.columns = [
        "CHANNEL",
        "ID",
        "ACTION",
        "ACTIVITY_DATE",
        "HCP_ID",
        "INDCTN_ID",
        "PRODUCT_NM",
    ]

    df["ACTION"] = df["ACTION"].replace({"Sent": "Delivered"})
    df["CHANNEL"] = df["CHANNEL"].replace(CHANNEL_MAPPING)

    if product_name:
        df = df[df["PRODUCT_NM"] == product_name]

    return df


def _build_ecare(edetail: pd.DataFrame, product_name: str) -> pd.DataFrame:
    """Build ecare records from edetail data (CARENET and MEDPEER channels).

    Args:
        edetail: Normalized edetail dataframe
        product_name: Product name to assign

    Returns:
        Processed ecare dataframe with exposure and engagement metrics
    """
    ecare = edetail[
        (edetail["CHANNEL"] == "EDETAIL_CARENET")
        | (edetail["CHANNEL"] == "EDETAIL_MEDPEER")
    ].copy()

    if ecare.empty:
        return ecare

    ecare = (
        ecare.pivot_table(
            index=["ACTIVITY_DATE", "HCP_ID", "ID", "CHANNEL"],
            columns="ACTION",
            values="PRODUCT_NM",
            aggfunc="count",
        )
        .reset_index()
        .fillna(0)
    )

    ecare["EXP"] = np.select(
        [(ecare.get("Delivered", 0) == 1)], ["Delivered"], default=""
    )
    ecare["ENG1"] = np.select([(ecare.get("Opened", 0) == 1)], ["Opened"], default="")

    ecare1 = pd.melt(
        ecare,
        id_vars=["ACTIVITY_DATE", "HCP_ID", "ID", "CHANNEL"],
        value_vars=["EXP", "ENG1"],
        var_name="ACTION2",
        value_name="ACTION",
    )
    ecare1 = ecare1[ecare1["ACTION"] != ""]
    ecare1.drop_duplicates(inplace=True)
    ecare1["PRODUCT_NM"] = product_name

    return ecare1[["ACTIVITY_DATE", "HCP_ID", "ID", "CHANNEL", "PRODUCT_NM", "ACTION"]]


def _build_m3(edetail: pd.DataFrame, product_name: str) -> pd.DataFrame:
    """Build M3 email records from edetail data (M3 channels).

    Args:
        edetail: Normalized edetail dataframe
        product_name: Product name to assign

    Returns:
        Processed M3 dataframe with exposure and engagement metrics
    """
    m3 = edetail[
        (edetail["CHANNEL"] == "EMAIL_M3_MR_KUN")
        | (edetail["CHANNEL"] == "EMAIL_M3_OPD")
        | (edetail["CHANNEL"] == "EMAIL_M3_QUIZ")
        | (edetail["CHANNEL"] == "EMAIL_M3_MM")
    ].copy()

    if m3.empty:
        return m3

    m = (
        pd.pivot_table(
            m3,
            index=["ACTIVITY_DATE", "HCP_ID", "ID", "CHANNEL"],
            columns="ACTION",
            values="PRODUCT_NM",
            aggfunc="count",
        )
        .reset_index()
        .fillna(0)
    )

    m["EXP"] = np.select([(m.get("Delivered", 0) == 1)], ["Delivered"], default="")
    m["ENG1"] = np.select([(m.get("Opened", 0) == 1)], ["Opened"], default="")
    m["ENG2"] = np.select([(m.get("Clicked", 0) == 1)], ["Clicked"], default="")

    m2 = pd.melt(
        m,
        id_vars=["ACTIVITY_DATE", "HCP_ID", "ID", "CHANNEL"],
        value_vars=["EXP", "ENG1", "ENG2"],
        var_name="ACTION2",
        value_name="ACTION",
    )
    m2 = m2[m2["ACTION"] != ""]
    m2["PRODUCT_NM"] = product_name

    return m2[["ACTIVITY_DATE", "HCP_ID", "ID", "CHANNEL", "PRODUCT_NM", "ACTION"]]


def _build_nmo(edetail: pd.DataFrame) -> pd.DataFrame:
    """Build NMO records from edetail data.

    Args:
        edetail: Normalized edetail dataframe

    Returns:
        NMO dataframe with 'Viewed' actions converted to 'Opened'
    """
    nmo = edetail[edetail["CHANNEL"] == "EDETAIL_NMO"].copy()
    if nmo.empty:
        return nmo
    nmo["ACTION"] = nmo["ACTION"].replace("Viewed", "Opened")
    return nmo


def run(config: Dict[str, Any]) -> pd.DataFrame:
    """Process edetail data and output standardized digital engagement records.

    Args:
        config: Configuration dictionary containing source path, filters, and output settings.
               Expected keys: source.path, filters.product_name,
               filters.months_to_retain, output.csv

    Returns:
        DataFrame with columns: HCP_ID, ACTIVITY_DATE, YRMO, ID, CHANNEL, ACTION
    """
    logger = get_logger(__name__)

    # Load and normalize source data
    df = read_table(config["source"]["path"])

    # Filter and normalize by product
    product_name = config.get("filters", {}).get("product_name", "EBG")
    edetail = _normalize_edetail(df, product_name)

    # Build channel-specific dataframes
    ecare1 = _build_ecare(edetail, product_name)
    m2 = _build_m3(edetail, product_name)
    nmo = _build_nmo(edetail)

    # Combine all channel data
    listcol = ["ACTIVITY_DATE", "HCP_ID", "ID", "CHANNEL", "ACTION", "PRODUCT_NM"]
    frames = []
    if not ecare1.empty:
        frames.append(ecare1[listcol])
    if not m2.empty:
        frames.append(m2[listcol])
    if not nmo.empty:
        frames.append(nmo[listcol])

    edetail_all = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=listcol)
    )

    # Add year-month column
    add_yrmo(edetail_all, "ACTIVITY_DATE")

    # Filter to retain only recent months
    months_to_retain = config.get("filters", {}).get("months_to_retain", 7)
    edetail_all = filter_by_yrmo_retention(edetail_all, months_to_retain)

    # Filter to keep only records where something was delivered
    delivered_ids = edetail_all[edetail_all["ACTION"] == "Delivered"]["ID"]
    finalp = edetail_all[edetail_all["ID"].isin(delivered_ids)].copy()

    df_new = finalp[["HCP_ID", "ACTIVITY_DATE", "YRMO", "ID", "CHANNEL", "ACTION"]]

    output_path = config["output"]["csv"]
    ensure_parent_dir(output_path)
    df_new.to_csv(output_path, index=False)
    logger.info("Saved Edetail output to %s", output_path)

    return df_new
