import pandas as pd

GINI_EXCLUDE = {"nov"}


def calculate_gini_index(combined_data, pdrb_df):
    """
    For each group (excluding nov):
      - Filter rows with area_category == 'all_area'
      - For each (id_indicator, year), compute Gini index across provinces using:
          1. value_num / nov  -> per number_of_villages (all_area)
          2. value_num / pdrb -> per economic output

    Returns combined_data unchanged plus two new keys:
      'gini_per_nov'  -> DataFrame: id_indicator, year, gini
      'gini_per_pdrb' -> DataFrame: id_indicator, year, gini
    """
    result = dict(combined_data)

    # Build nov lookup: (id_province, year) -> number_of_villages for all_area
    nov_df = combined_data.get("nov")
    if nov_df is None:
        raise KeyError("'nov' not found in combined_data — number of villages data is required")

    nov_lookup = (
        nov_df[nov_df["area_category"] == "all_area"]
        .set_index(["id_province", "year"])["value"]
        .to_dict()
    )

    # Build pdrb lookup: (id_province, year) -> pdrb
    pdrb_lookup = (
        pdrb_df
        .set_index(["id_province", "year"])["pdrb"]
        .to_dict()
    )

    gini_nov_parts = []
    gini_pdrb_parts = []

    for group_key, df in combined_data.items():
        if group_key in GINI_EXCLUDE:
            continue

        if "value_num" not in df.columns:
            continue

        all_area = df[df["area_category"] == "all_area"].copy()
        if all_area.empty:
            continue

        for (id_indicator, year), group in all_area.groupby(["id_indicator", "year"]):
            group = group.copy()

            vals_per_nov = group.apply(
                lambda row: _normalize(
                    row["value_num"],
                    nov_lookup.get((row["id_province"], row["year"]))
                ),
                axis=1,
            ).tolist()

            vals_per_pdrb = group.apply(
                lambda row: _normalize(
                    row["value_num"],
                    pdrb_lookup.get((row["id_province"], row["year"]))
                ),
                axis=1,
            ).tolist()

            gini_nov = _gini(vals_per_nov)
            gini_pdrb = _gini(vals_per_pdrb)

            if gini_nov is not None:
                gini_nov_parts.append({
                    "id_indicator": id_indicator,
                    "year": year,
                    "gini": gini_nov,
                })

            if gini_pdrb is not None:
                gini_pdrb_parts.append({
                    "id_indicator": id_indicator,
                    "year": year,
                    "gini": gini_pdrb,
                })

        print(f"  [{group_key}] gini index calculated")

    if gini_nov_parts:
        gini_nov_df = (
            pd.DataFrame(gini_nov_parts)
            .sort_values(["id_indicator", "year"])
            .reset_index(drop=True)
        )
        result["gini_per_nov"] = gini_nov_df
        print(f"  -> gini_per_nov ({len(gini_nov_df)} rows total)")

    if gini_pdrb_parts:
        gini_pdrb_df = (
            pd.DataFrame(gini_pdrb_parts)
            .sort_values(["id_indicator", "year"])
            .reset_index(drop=True)
        )
        result["gini_per_pdrb"] = gini_pdrb_df
        print(f"  -> gini_per_pdrb ({len(gini_pdrb_df)} rows total)")

    # Per-province Gini (ketimpangan antar indikator dalam satu provinsi-area)
    gini_province_df = _calculate_gini_per_province(combined_data)
    if not gini_province_df.empty:
        result["gini_per_province"] = gini_province_df
        print(f"  -> gini_per_province ({len(gini_province_df)} rows total)")

    return result


def _calculate_gini_per_province(combined_data):
    """
    For each (id_province, area_category, year):
      - Collect value_pct across all indicators from all groups (excluding nov)
      - Compute Gini index across those indicator values

    Returns a pivot DataFrame:
      Rows    : id_province
      Columns : MultiIndex (year, area_category) ordered urban → rural → all_area
      Values  : Gini coefficient
    """
    AREA_ORDER = ["urban", "rural", "all_area"]

    parts = []
    for group_key, df in combined_data.items():
        if group_key in GINI_EXCLUDE:
            continue
        if "value_pct" not in df.columns:
            continue
        subset = df[
            (df["id_province"] > 0) &          # exclude national aggregate (id_province=0)
            (df["area_category"] == "all_area") # gini focuses on all_area only
        ][["id_province", "area_category", "year", "id_indicator", "value_pct"]].copy()
        parts.append(subset)

    if not parts:
        return pd.DataFrame()

    stacked = pd.concat(parts, ignore_index=True)

    records = []
    for (id_province, area_category, year), group in stacked.groupby(
        ["id_province", "area_category", "year"]
    ):
        gini_val = _gini(group["value_pct"].tolist())
        if gini_val is not None:
            records.append({
                "id_province": id_province,
                "area_category": area_category,
                "year": year,
                "gini": gini_val,
            })

    if not records:
        return pd.DataFrame()

    long_df = pd.DataFrame(records)

    # Pivot: rows = id_province, columns = year
    pivot = long_df.pivot_table(
        index="id_province",
        columns="year",
        values="gini",
    )
    pivot.columns.name = None
    pivot.index.name = "id_province"

    return pivot.reset_index()


def _normalize(value, denominator):
    if value is None or pd.isna(value):
        return None
    if denominator is None or pd.isna(denominator) or denominator == 0:
        return None
    return value / denominator


def _gini(values):
    """
    Gini coefficient for non-negative values across provinces.

    Formula (sorted ascending, 1-indexed rank i):
      G = (2 * Σ(i * x_i)) / (n * Σ x_i) - (n+1)/n

    Returns None if fewer than 2 valid values.
    Returns 0.0 if all values are 0.
    """
    clean = [v for v in values if v is not None and not pd.isna(v) and v >= 0]
    n = len(clean)
    if n < 2:
        return None
    clean = sorted(clean)
    total = sum(clean)
    if total == 0:
        return 0.0
    weighted = sum((i + 1) * v for i, v in enumerate(clean))
    return round((2 * weighted) / (n * total) - (n + 1) / n, 4)
