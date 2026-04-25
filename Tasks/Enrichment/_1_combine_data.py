import os
import glob
import pandas as pd
import re


def has_split_pattern(filename):
    """
    Detect files like 'aip - 1.xlsx'
    """
    return re.search(r"\s-\s\d+", filename) is not None


def remove_split_suffix(filename):
    """
    'aip - 1.xlsx' → 'aip.xlsx'
    """
    return re.sub(r"\s*-\s*\d+", "", filename.lower())


def get_group_key(filename):
    """
    NEW RULE:
    Group by:
    1. remove ' - 1' pattern
    2. take prefix before '_'

    Examples:
    ai_rural.xlsx → ai
    ai_urban.xlsx → ai
    ai.xlsx → ai
    aip - 1.xlsx → aip
    """
    name = filename.lower().replace(".xlsx", "")

    # STEP 1: remove split pattern
    name = re.sub(r"\s*-\s*\d+", "", name)

    # STEP 2: take prefix before "_"
    if "_" in name:
        name = name.split("_")[0]

    return name


def combine_data(folder_path):
    files = glob.glob(os.path.join(folder_path, "*.xlsx"))

    # Exclude files matching total_penduduk* and pdrb_*
    files = [
        f for f in files
        if not re.match(r"total_penduduk|pdrb_", os.path.basename(f).lower())
    ]

    grouped_files = {}

    # STEP 1: Group files
    for file_path in files:
        filename = os.path.basename(file_path)
        group_key = get_group_key(filename)

        grouped_files.setdefault(group_key, []).append(file_path)

    combined_data = {}

    # STEP 2: Combine each group
    for group_key, file_list in grouped_files.items():

        df_list = []

        for file_path in file_list:
            df = pd.read_excel(file_path)
            df_list.append(df)

        combined_df = pd.concat(df_list, ignore_index=True)

        combined_data[group_key] = combined_df

    return combined_data