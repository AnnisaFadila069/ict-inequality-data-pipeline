import pandas as pd

RATIO_GROUPS = {"cps", "bts", "ifc"}


def calculate_ratio(combined_data):
    """
    For number-based groups (cps, bts, ifc):
      - Filter rows with area_category == 'urban' OR 'rural'
      - Match by (id_province, id_indicator, year)
      - ratio = urban_pct / rural_pct
        (value_pct already normalized per number_of_villages)

    Returns combined_data unchanged (pass-through) plus one new key:
      'ratio_urban_rural' -> DataFrame with columns:
        id_province, id_indicator, year, ratio
      (all three groups merged into a single table)
    """
    result = dict(combined_data)

    ratio_parts = []

    for group_key, df in combined_data.items():
        if group_key not in RATIO_GROUPS:
            continue

        df = df.copy()

        urban = df[df["area_category"] == "urban"][
            ["id_province", "id_indicator", "year", "value_pct"]
        ].rename(columns={"value_pct": "urban_pct"})

        rural = df[df["area_category"] == "rural"][
            ["id_province", "id_indicator", "year", "value_pct"]
        ].rename(columns={"value_pct": "rural_pct"})

        merged = pd.merge(
            urban,
            rural,
            on=["id_province", "id_indicator", "year"],
            how="inner",
        )

        merged["ratio"] = merged.apply(
            lambda row: _safe_ratio(row["urban_pct"], row["rural_pct"]),
            axis=1,
        )

        ratio_parts.append(
            merged[["id_province", "id_indicator", "year", "ratio"]]
        )

        print(f"  [{group_key}] ratio calculated ({len(merged)} rows)")

    if ratio_parts:
        ratio_df = (
            pd.concat(ratio_parts, ignore_index=True)
            .sort_values(["id_province", "id_indicator", "year"])
            .reset_index(drop=True)
        )
        result["ratio_urban_rural"] = ratio_df
        print(f"  -> ratio_urban_rural combined ({len(ratio_df)} rows total)")

    return result


def _safe_ratio(urban_pct, rural_pct):
    if pd.isna(urban_pct) or pd.isna(rural_pct):
        return None
    if rural_pct == 0:
        return None
    return round(urban_pct / rural_pct, 4)
