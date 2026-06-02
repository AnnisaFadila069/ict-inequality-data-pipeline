import pandas as pd

GINI_GROUPS = {"cps", "bts", "ifc"}


def _gini_coefficient(values):
    """
    Discrete Gini Coefficient via Lorenz curve.
    G = (2 * sum(rank_i * x_i)) / (n * sum(x_i)) - (n+1)/n
    Sorted ascending; ranks are 1-indexed.
    Returns None if input is empty or sum is zero.
    """
    arr = sorted(v for v in values if pd.notna(v) and v >= 0)
    n = len(arr)
    if n == 0:
        return None
    total = sum(arr)
    if total == 0:
        return 0.0
    gini = (2 * sum((i + 1) * x for i, x in enumerate(arr))) / (n * total) - (n + 1) / n
    return round(gini, 4)


def calculate_gini_index(combined_data, pdrb_df):
    """
    Step 1 — compute normalized ratio per (province, year, indicator):
      'village': value_num / nov_all_area
      'pdrb':    value_num / pdrb (regional GDP)
    Only uses all_area rows from GINI_GROUPS.

    Step 2 — compute Gini Coefficient for Indonesia from the
    distribution of ratios across provinces per (year, indicator,
    normalization_type).

    Returns combined_data with two new keys:
      'normalization_province' -> id_province, year, id_indicator,
                                   normalization_type, ratio
      'gini_indonesia'         -> year, id_indicator,
                                   normalization_type, gini
    """
    result = dict(combined_data)

    nov_df = combined_data.get("nov")
    if nov_df is None:
        raise KeyError("'nov' not found in combined_data")

    nov_lookup = (
        nov_df[nov_df["area_category"] == "all_area"]
        .set_index(["id_province", "year"])["value"]
        .to_dict()
    )

    pdrb_lookup = pdrb_df.set_index(["id_province", "year"])["pdrb"].to_dict()

    ratio_rows = []

    for group_key, df in combined_data.items():
        if group_key not in GINI_GROUPS:
            continue

        all_area = df[df["area_category"] == "all_area"].copy()
        if all_area.empty:
            continue

        for _, row in all_area.iterrows():
            prov = row["id_province"]
            year = row["year"]
            ind = row["id_indicator"]
            value_num = row["value_num"]

            if pd.isna(value_num) or value_num < 0:
                continue

            nov_val = nov_lookup.get((prov, year))
            if nov_val is not None and not pd.isna(nov_val) and nov_val > 0:
                ratio_rows.append({
                    "id_province":       prov,
                    "year":              year,
                    "id_indicator":      ind,
                    "normalization_type": "village",
                    "ratio":             round(value_num / nov_val, 6),
                })

            pdrb_val = pdrb_lookup.get((prov, year))
            if pdrb_val is not None and not pd.isna(pdrb_val) and pdrb_val > 0:
                ratio_rows.append({
                    "id_province":       prov,
                    "year":              year,
                    "id_indicator":      ind,
                    "normalization_type": "pdrb",
                    "ratio":             round(value_num / pdrb_val, 6),
                })

        print(f"  [{group_key}] normalization ratios calculated")

    if not ratio_rows:
        return result

    norm_df = (
        pd.DataFrame(ratio_rows)
        .sort_values(["id_province", "year", "id_indicator", "normalization_type"])
        .reset_index(drop=True)
    )
    result["normalization_province"] = norm_df
    print(f"  -> normalization_province ({len(norm_df)} rows total)")

    gini_rows = []
    for (year, ind, norm_type), grp in norm_df.groupby(
        ["year", "id_indicator", "normalization_type"]
    ):
        g = _gini_coefficient(grp["ratio"].tolist())
        if g is not None:
            gini_rows.append({
                "year":               year,
                "id_indicator":       ind,
                "normalization_type": norm_type,
                "gini":               g,
            })

    if gini_rows:
        gini_df = (
            pd.DataFrame(gini_rows)
            .sort_values(["year", "id_indicator", "normalization_type"])
            .reset_index(drop=True)
        )
        result["gini_indonesia"] = gini_df
        print(f"  -> gini_indonesia ({len(gini_df)} rows total)")

    return result
