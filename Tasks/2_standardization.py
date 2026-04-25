import os
import glob
import pandas as pd
from pathlib import Path

from Standarization._1_transform_to_long import transform_to_long
from Standarization._2_add_year_column import add_year_column
from Standarization._3_add_area_category import add_area_category

FILES_SKIP_LONG = {"pdrb"}
FILES_SKIP_AREA = {"pdrb"}

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
    print("STANDARIZATION PIPELINE")
    print(f"Source : {input_folder}")
    print(f"Output : {output_folder}")
    print("=" * 60 + "\n")


def run_standardization():

    env = load_env_auto()

    input_folder = env.get("BASE_FOLDER_CLEAN_DATA")
    output_folder = env.get("BASE_FOLDER_STANDARDIZED_DATA")
    year_env = env.get("YEAR")

    if not input_folder or not output_folder or not year_env:
        raise ValueError("Missing required env variables")

    excel_files = glob.glob(os.path.join(input_folder, "*.xlsx"))

    if not excel_files:
        print(f"No .xlsx files found in: {input_folder}")
        return

    os.makedirs(output_folder, exist_ok=True)

    print_header(input_folder, output_folder)
    
    # =========================
    # LOAD ALL FILES FIRST
    # =========================
    datasets = []
    for input_path in sorted(excel_files):
        filename = os.path.basename(input_path)
        df = pd.read_excel(input_path)
        datasets.append((filename, input_path, df))

    # =========================
    # STEP 1
    # =========================
    print("STEP 1: TRANSFORM TO LONG FORMAT")

    processed = []

    for filename, input_path, df in datasets:
        filename_lower = filename.lower()

        if any(x in filename_lower for x in FILES_SKIP_LONG):
            print(f"  {filename} → skipped (wide format)")
        else:
            df = transform_to_long(df, filename)
            print(f"  {filename} → success")

        processed.append((filename, df))

    print("STEP 1 COMPLETED\n")

    # =========================
    # STEP 2
    # =========================
    print("STEP 2: ADD YEAR COLUMN")

    processed2 = []

    for filename, df in processed:
        df = add_year_column(df, filename, year_env)
        processed2.append((filename, df))
        print(f"  {filename} → year added")

    print("STEP 2 COMPLETED\n")

    # =========================
    # STEP 3
    # =========================
    print("STEP 3: ADD AREA CATEGORY")

    processed3 = []

    for filename, df in processed2:
        filename_lower = filename.lower()

        if any(x in filename_lower for x in FILES_SKIP_AREA):
            print(f"  {filename} → skipped (no area)")
        else:
            df = add_area_category(df, filename)
            print(f"  {filename} → area added")

        processed3.append((filename, df))

    print("STEP 3 COMPLETED\n")

    # =========================
    # STEP 4 + SAVE
    # =========================
    print("STEP 4: SAVE FILES")

    for filename, df in processed3:
        output_path = os.path.join(output_folder, filename)

        if "value" in df.columns:
            cols = [c for c in df.columns if c != "value"] + ["value"]
            df = df[cols]

        df.to_excel(output_path, index=False)
        print(f"  {filename} → saved")

    print("\nPIPELINE COMPLETED")


if __name__ == "__main__":
    run_standardization()