import os
import glob
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
# HELPERS
# =============================================================================

def read_excel(path):
    return pd.read_excel(path)


def save(df, folder, filename):
    os.makedirs(folder, exist_ok=True)
    out = os.path.join(folder, filename)
    df.to_excel(out, index=False)
    print(f"  {filename} -> {len(df)} rows saved")


def print_header(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


# =============================================================================
# DIMENSION LOADERS
# =============================================================================

def load_dimensions(dim_folder, out_folder):
    print_header("LOAD DIMENSIONS")

    dim_files = glob.glob(os.path.join(dim_folder, "dim_*.xlsx"))
    if not dim_files:
        raise FileNotFoundError(f"No dim_*.xlsx files found in: {dim_folder}")

    for file_path in sorted(dim_files):
        filename = os.path.basename(file_path)
        df = read_excel(file_path)
        save(df, out_folder, filename)


# =============================================================================
# FACT BUILDERS
# =============================================================================

ICT_GROUPS = {"ai", "aia", "aie", "ail", "aip", "bts", "cps", "ifc"}

FACT_ICT_COLS     = ["id_province", "id_indicator", "year", "area_category", "value_pct", "value_num"]
FACT_RATIO_COLS   = ["id_province", "id_indicator", "year", "ratio"]
FACT_GINI_COLS    = ["id_indicator", "year", "id_method", "gini"]
FACT_GINI_P_COLS  = ["id_province", "year", "gini"]
FACT_NOV_COLS     = ["id_province", "area_category", "year", "number_of_villages"]
FACT_POP_COLS     = ["id_province", "area_category", "year", "total_population"]
FACT_PDRB_COLS    = ["id_province", "year", "pdrb"]


def build_fact_ict(enriched_folder):
    """Merge all ICT group files into one fact table."""
    parts = []
    for group in sorted(ICT_GROUPS):
        path = os.path.join(enriched_folder, f"{group}.xlsx")
        if not os.path.exists(path):
            print(f"    [WARN] {group}.xlsx not found, skipped")
            continue
        df = read_excel(path)
        parts.append(df)

    if not parts:
        raise FileNotFoundError("No ICT group files found")

    fact = (
        pd.concat(parts, ignore_index=True)
        .reindex(columns=FACT_ICT_COLS)
        .sort_values(["id_province", "id_indicator", "year", "area_category"])
        .reset_index(drop=True)
    )
    return fact


def build_fact_ratio(enriched_folder):
    """Load ratio_urban_rural, exclude national aggregate (id_province=0)."""
    path = os.path.join(enriched_folder, "ratio_urban_rural.xlsx")
    df = read_excel(path)
    df = df[df["id_province"] != 0].copy()
    return (
        df.reindex(columns=FACT_RATIO_COLS)
        .sort_values(["id_province", "id_indicator", "year"])
        .reset_index(drop=True)
    )


def build_fact_gini(enriched_folder):
    """
    Combine gini_per_nov (id_method=2) and gini_per_pdrb (id_method=3)
    into one long-format fact table.

    id_method matches dim_normalization:
      2 = Per Village (nov)
      3 = Per GDP (PDRB)
    """
    nov_df  = read_excel(os.path.join(enriched_folder, "gini_per_nov.xlsx")).assign(id_method=2)
    pdrb_df = read_excel(os.path.join(enriched_folder, "gini_per_pdrb.xlsx")).assign(id_method=3)

    return (
        pd.concat([nov_df, pdrb_df], ignore_index=True)
        .reindex(columns=FACT_GINI_COLS)
        .sort_values(["id_indicator", "year", "id_method"])
        .reset_index(drop=True)
    )


def build_fact_gini_province(enriched_folder):
    """
    Convert gini_per_province pivot (id_province × year columns)
    into long format: id_province, year, gini.
    """
    path = os.path.join(enriched_folder, "gini_per_province.xlsx")
    df = read_excel(path)

    year_cols = [c for c in df.columns if c != "id_province"]
    fact = df.melt(
        id_vars="id_province",
        value_vars=year_cols,
        var_name="year",
        value_name="gini",
    )
    fact["year"] = pd.to_numeric(fact["year"], errors="coerce").astype("Int64")
    fact = fact.dropna(subset=["gini"])

    return (
        fact.reindex(columns=FACT_GINI_P_COLS)
        .sort_values(["id_province", "year"])
        .reset_index(drop=True)
    )


def build_fact_nov(enriched_folder):
    """Number of villages per province × area × year."""
    df = read_excel(os.path.join(enriched_folder, "nov.xlsx"))
    df = df.rename(columns={"value": "number_of_villages"})
    return (
        df.reindex(columns=FACT_NOV_COLS)
        .sort_values(["id_province", "area_category", "year"])
        .reset_index(drop=True)
    )


def build_fact_population(enriched_folder):
    """Total population per province × area × year."""
    files = glob.glob(os.path.join(enriched_folder, "total_penduduk*.xlsx"))
    if not files:
        raise FileNotFoundError("total_penduduk*.xlsx not found")
    df = read_excel(files[0])
    df = df.rename(columns={"value": "total_population"})
    return (
        df.reindex(columns=FACT_POP_COLS)
        .sort_values(["id_province", "area_category", "year"])
        .reset_index(drop=True)
    )


def build_fact_pdrb(enriched_folder):
    """PDRB (regional GDP) per province × year."""
    files = glob.glob(os.path.join(enriched_folder, "pdrb_*.xlsx"))
    if not files:
        raise FileNotFoundError("pdrb_*.xlsx not found")
    df = read_excel(files[0])
    return (
        df.reindex(columns=FACT_PDRB_COLS)
        .sort_values(["id_province", "year"])
        .reset_index(drop=True)
    )


# =============================================================================
# FACT LOADER
# =============================================================================

def load_facts(enriched_folder, out_folder):
    print_header("LOAD FACTS")

    tasks = [
        ("fact_ict.xlsx",           build_fact_ict),
        ("fact_ratio.xlsx",         build_fact_ratio),
        ("fact_gini.xlsx",          build_fact_gini),
        ("fact_gini_province.xlsx", build_fact_gini_province),
        ("fact_nov.xlsx",           build_fact_nov),
        ("fact_population.xlsx",    build_fact_population),
        ("fact_pdrb.xlsx",          build_fact_pdrb),
    ]

    for filename, builder in tasks:
        try:
            df = builder(enriched_folder)
            save(df, out_folder, filename)
        except Exception as e:
            print(f"  [ERROR] {filename}: {e}")


# =============================================================================
# MAIN
# =============================================================================

def run_load():
    env = load_env_auto()

    enriched_folder = env.get("BASE_FOLDER_ENRICHED_DATA")
    dim_folder      = env.get("BASE_FOLDER_DIMENSION_DATA")
    out_folder      = env.get("BASE_FOLDER_LOAD_DATA")

    for key, val in [
        ("BASE_FOLDER_ENRICHED_DATA",  enriched_folder),
        ("BASE_FOLDER_DIMENSION_DATA", dim_folder),
        ("BASE_FOLDER_LOAD_DATA",      out_folder),
    ]:
        if not val:
            raise ValueError(f"Missing env variable: {key}")

    print("\n" + "=" * 60)
    print("LOAD PIPELINE")
    print(f"  Enriched : {enriched_folder}")
    print(f"  Dimension: {dim_folder}")
    print(f"  Output   : {out_folder}")
    print("=" * 60)

    load_dimensions(dim_folder, out_folder)
    load_facts(enriched_folder, out_folder)

    print("\nLOAD PIPELINE COMPLETED")


if __name__ == "__main__":
    run_load()
