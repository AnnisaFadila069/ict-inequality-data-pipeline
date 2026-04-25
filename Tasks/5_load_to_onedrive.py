import os
import shutil
import pandas as pd
from pathlib import Path


# =============================================================================
# ENV LOADER
# =============================================================================

def load_env_auto():
    current = Path(__file__).resolve()
    for parent in current.parents:
        env_path = parent / ".env"
        if env_path.exists():
            env = {}
            with open(env_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    env[key.strip()] = value.strip().strip('"').strip("'")
            return env
    raise FileNotFoundError(".env file not found")


# =============================================================================
# PRIMARY KEY REGISTRY
# Defines which columns uniquely identify a row in each file.
# Used for upsert: new data wins on conflict.
# =============================================================================

PRIMARY_KEYS = {
    # Dimensions
    "dim_province.xlsx":      ["id_province"],
    "dim_indicator.xlsx":     ["id_indicator"],
    "dim_area.xlsx":          ["area_category"],
    "dim_year.xlsx":          ["year"],
    "dim_normalization.xlsx": ["id_method"],

    # Facts
    "fact_ict.xlsx":           ["id_province", "id_indicator", "year", "area_category"],
    "fact_ratio.xlsx":         ["id_province", "id_indicator", "year"],
    "fact_gini.xlsx":          ["id_indicator", "year", "id_method"],
    "fact_gini_province.xlsx": ["id_province", "year"],
    "fact_nov.xlsx":           ["id_province", "area_category", "year"],
    "fact_population.xlsx":    ["id_province", "area_category", "year"],
    "fact_pdrb.xlsx":          ["id_province", "year"],
}


# =============================================================================
# UPSERT LOGIC
# =============================================================================

def upsert(existing_path, new_df, pk_cols):
    """
    Merge new_df into the existing Excel file at existing_path.

    Strategy:
      1. Read existing data.
      2. Concatenate existing + new (new appended last).
      3. Drop duplicates on pk_cols, keeping the LAST occurrence (= new data wins).
      4. Sort by pk_cols and write back.

    Returns the merged DataFrame.
    """
    existing_df = pd.read_excel(existing_path)

    # Align column order to existing file
    cols = existing_df.columns.tolist()
    new_df = new_df.reindex(columns=cols)

    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=pk_cols, keep="last")

    # Sort safely: cast PK cols to string to handle mixed int/str types
    sort_key = combined[pk_cols].astype(str).apply(tuple, axis=1)
    combined = combined.iloc[sort_key.argsort()].reset_index(drop=True)
    merged = combined

    inserted = len(merged) - len(existing_df)
    updated  = len(new_df) - max(inserted, 0)

    return merged, inserted, updated


# =============================================================================
# MAIN UPLOAD FUNCTION
# =============================================================================

def upload_file(src_path, dst_path, pk_cols):
    """
    Upsert a single file from src_path to dst_path.

    - If dst does not exist: copy directly (INSERT all rows).
    - If dst exists: upsert using pk_cols (UPDATE/INSERT).
    """
    filename = os.path.basename(src_path)
    new_df   = pd.read_excel(src_path)

    if not os.path.exists(dst_path):
        shutil.copy2(src_path, dst_path)
        print(f"  [NEW]    {filename} -> {len(new_df)} rows inserted")
        return

    merged, inserted, updated = upsert(dst_path, new_df, pk_cols)
    merged.to_excel(dst_path, index=False)
    print(f"  [UPSERT] {filename} -> +{inserted} inserted, ~{updated} updated ({len(merged)} total)")


# =============================================================================
# PIPELINE
# =============================================================================

def run_load_to_onedrive():
    env = load_env_auto()

    src_folder = env.get("BASE_FOLDER_LOAD_DATA")
    dst_folder = env.get("BASE_FOLDER_ONE_DRIVE")

    for key, val in [
        ("BASE_FOLDER_LOAD_DATA",  src_folder),
        ("BASE_FOLDER_ONE_DRIVE",  dst_folder),
    ]:
        if not val:
            raise ValueError(f"Missing env variable: {key}")

    os.makedirs(dst_folder, exist_ok=True)

    print("\n" + "=" * 60)
    print("LOAD TO ONEDRIVE")
    print(f"  Source : {src_folder}")
    print(f"  Target : {dst_folder}")
    print("=" * 60 + "\n")

    # Process files in registry order: dims first, then facts
    missing  = []
    skipped  = []

    for filename, pk_cols in PRIMARY_KEYS.items():
        src_path = os.path.join(src_folder, filename)
        dst_path = os.path.join(dst_folder, filename)

        if not os.path.exists(src_path):
            missing.append(filename)
            continue

        try:
            upload_file(src_path, dst_path, pk_cols)
        except Exception as e:
            print(f"  [ERROR]  {filename}: {e}")
            skipped.append(filename)

    if missing:
        print(f"\n  [WARN] Not found in source, skipped: {missing}")
    if skipped:
        print(f"  [WARN] Failed to upload: {skipped}")

    print("\nLOAD TO ONEDRIVE COMPLETED")


if __name__ == "__main__":
    run_load_to_onedrive()
