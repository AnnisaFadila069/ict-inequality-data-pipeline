import os
import glob
import re
import shutil
from pathlib import Path

import pandas as pd

from Enrichment._1_combine_data import combine_data
from Enrichment._2_convert_number_to_percentage import convert_number_to_percentage
from Enrichment._3__convert_percentage_to_number import convert_percentage_to_number
from Enrichment._4_calculate_ratio import calculate_ratio
from Enrichment._5_calculate_gini_index import calculate_gini_index


def load_env(env_path):
    env = {}
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def load_env_auto():
    current = Path(__file__).resolve()

    for parent in current.parents:
        env_path = parent / ".env"
        if env_path.exists():
            return load_env(env_path)

    raise FileNotFoundError(".env file not found")


def print_header(input_folder, output_folder):
    print("\n" + "=" * 60)
    print("ENRICHMENT PIPELINE")
    print(f"Source : {input_folder}")
    print(f"Output : {output_folder}")
    print("=" * 60 + "\n")


def run_enrichment():

    env = load_env_auto()

    input_folder = env.get("BASE_FOLDER_STANDARDIZED_DATA")
    output_folder = env.get("BASE_FOLDER_ENRICHED_DATA")

    if not input_folder or not output_folder:
        raise ValueError("Missing required env variables: BASE_FOLDER_STANDARDIZED_DATA, BASE_FOLDER_ENRICHED_DATA")

    excel_files = glob.glob(os.path.join(input_folder, "*.xlsx"))

    if not excel_files:
        print(f"No .xlsx files found in: {input_folder}")
        return

    os.makedirs(output_folder, exist_ok=True)

    print_header(input_folder, output_folder)

    # =========================
    # STEP 1: COMBINE DATA
    # =========================
    print("STEP 1: COMBINE DATA")

    combined = combine_data(input_folder)

    for group_key, df in combined.items():
        print(f"  {group_key} -> combined ({len(df)} rows)")

    print("STEP 1 COMPLETED\n")

    # =========================
    # LOAD REFERENCE DATA
    # =========================
    total_penduduk_files = glob.glob(os.path.join(input_folder, "total_penduduk*.xlsx"))
    if not total_penduduk_files:
        raise FileNotFoundError("total_penduduk*.xlsx not found in input folder")
    total_penduduk_df = pd.read_excel(total_penduduk_files[0])

    pdrb_files = glob.glob(os.path.join(input_folder, "pdrb_*.xlsx"))
    if not pdrb_files:
        raise FileNotFoundError("pdrb_*.xlsx not found in input folder")
    pdrb_df = pd.read_excel(pdrb_files[0])

    # =========================
    # STEP 2: CONVERT NUMBER TO PERCENTAGE
    # =========================
    print("STEP 2: CONVERT NUMBER TO PERCENTAGE")

    combined = convert_number_to_percentage(combined)

    print("STEP 2 COMPLETED\n")

    # =========================
    # STEP 3: CONVERT PERCENTAGE TO NUMBER 
    # =========================
    print("STEP 3: CONVERT PERCENTAGE TO NUMBER")

    combined = convert_percentage_to_number(combined, total_penduduk_df)

    print("STEP 3 COMPLETED\n")

    # =========================
    # STEP 4: CALCULATE RATIO
    # =========================
    print("STEP 4: CALCULATE RATIO")

    combined = calculate_ratio(combined)

    print("STEP 4 COMPLETED\n")

    # =========================
    # STEP 5: CALCULATE GINI INDEX
    # =========================
    print("STEP 5: CALCULATE GINI INDEX")

    combined = calculate_gini_index(combined, pdrb_df)

    print("STEP 5 COMPLETED\n")

    # =========================
    # STEP 6: RELABEL INDONESIA ROWS
    # =========================
    print("STEP 6: RELABEL INDONESIA ROWS")

    combined = _relabel_indonesia(combined)

    print("STEP 6 COMPLETED\n")

    # =========================
    # SAVE FILES
    # =========================
    print("SAVE FILES")

    for group_key, df in combined.items():
        output_path = os.path.join(output_folder, f"{group_key}.xlsx")
        df.to_excel(output_path, index=False)
        print(f"  {group_key}.xlsx -> saved")

    # Copy excluded files (total_penduduk* and pdrb_*) directly to output
    for file_path in excel_files:
        filename = os.path.basename(file_path)
        if re.match(r"total_penduduk|pdrb_", filename.lower()):
            output_path = os.path.join(output_folder, filename)
            shutil.copy2(file_path, output_path)
            print(f"  {filename} -> copied (excluded from combine)")

    print("\nPIPELINE COMPLETED")


def _relabel_indonesia(combined_data):
    """
    For rows where id_province == 0 (national aggregate):
      - area_category 'all_area' → 'indonesia'
      - area_category 'urban'    → 'indonesia_urban'
      - area_category 'rural'    → 'indonesia_rural'

    This allows dashboard filters to distinguish:
      urban / rural / all_area (province) / indonesia
    """
    AREA_REMAP = {
        "all_area": "indonesia",
        "urban":    "indonesia_urban",
        "rural":    "indonesia_rural",
    }

    result = {}
    for key, df in combined_data.items():
        if "id_province" not in df.columns or "area_category" not in df.columns:
            result[key] = df
            continue

        df = df.copy()
        mask = df["id_province"] == 0
        df.loc[mask, "area_category"] = df.loc[mask, "area_category"].map(
            lambda v: AREA_REMAP.get(v, v)
        )
        result[key] = df
        remapped = mask.sum()
        if remapped:
            print(f"  [{key}] {remapped} Indonesia rows relabeled")

    return result


if __name__ == "__main__":
    run_enrichment()
