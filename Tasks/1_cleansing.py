import os
import sys
import re
import glob
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from Cleansing import data_internet, pdrb, total_penduduk
from Cleansing.rename_indicators import rename_indicators, validate_no_conflicts

def _load_env(env_path):
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


def _get_prefix(filename):
    name = os.path.splitext(filename.lower())[0]       # strip .xlsx
    name = re.sub(r"\s*-\s*\d+", "", name).strip()    # strip ' - 1' etc
    return name.split("_")[0]


def _save(df, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_excel(output_path, index=False)
    print(f"  Saved : {output_path}")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.normpath(os.path.join(script_dir, "..", ".env"))
    env = _load_env(env_path)

    raw_folder = env.get("BASE_FOLDER_RAW_DATA")
    clean_folder = env.get("BASE_FOLDER_CLEAN_DATA")

    if not raw_folder or not clean_folder:
        raise ValueError(
            "BASE_FOLDER_RAW_DATA and BASE_FOLDER_CLEAN_DATA must be defined in .env"
        )

    all_files = sorted(glob.glob(os.path.join(raw_folder, "*")))
    if not all_files:
        print(f"No files found in: {raw_folder}")
        return

    print("=" * 60)
    print("CLEANSING PIPELINE")
    print(f"Source : {raw_folder}")
    print(f"Output : {clean_folder}")
    print("=" * 60)

    # Track indicator columns per prefix for conflict validation
    indicator_cols_by_prefix = {}

    for input_path in all_files:
        filename = os.path.basename(input_path)
        prefix = _get_prefix(filename)
        output_path = os.path.join(clean_folder, os.path.splitext(filename)[0] + ".xlsx")

        try:
            if prefix == "pdrb":
                df = pdrb.clean(input_path)
                _save(df, output_path)

            elif prefix == "total":
                df = total_penduduk.clean(input_path)
                _save(df, output_path)

            else:
                df_raw = pd.read_excel(input_path, header=None)
                df = data_internet.clean(df_raw, filename)
                df = rename_indicators(df, prefix)
                _save(df, output_path)

                # Record indicator columns per prefix (use any one file per prefix)
                indicator_cols_by_prefix[prefix] = [
                    c for c in df.columns if c != "id_province"
                ]

        except Exception as e:
            print(f"  ERROR : {e}")

    validate_no_conflicts(indicator_cols_by_prefix)

    print("=" * 60)
    print("Done.")

if __name__ == "__main__":
    main()
