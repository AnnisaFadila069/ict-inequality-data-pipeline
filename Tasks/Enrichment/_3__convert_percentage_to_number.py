import pandas as pd

PERCENTAGE_GROUPS = {"aip", "aia", "aie", "ai", "ail"}


def convert_percentage_to_number(combined_data, total_penduduk_df):
    """
    For percentage-based groups (aip, aia, aie, ai, ail):
    - Rename 'value' -> 'value_pct'
    - Add 'value_num' = value_pct / 100 * total_population
      matched by id_province and area_category

    Other groups are returned unchanged.
    """
    # Build lookup: (id_province, area_category) -> total_population
    pop_lookup = (
        total_penduduk_df
        .set_index(["id_province", "area_category"])["value"]
        .to_dict()
    )

    result = {}

    for group_key, df in combined_data.items():
        if group_key not in PERCENTAGE_GROUPS:
            result[group_key] = df
            continue

        df = df.copy()
        df = df.rename(columns={"value": "value_pct"})

        df["value_num"] = df.apply(
            lambda row: _calc_num(row, pop_lookup),
            axis=1,
        )

        print(f"  [{group_key}] percentage -> number conversion applied")
        result[group_key] = df

    return result


def _calc_num(row, pop_lookup):
    if pd.isna(row["value_pct"]):
        return None  # no data
    key = (row["id_province"], row["area_category"])
    total_pop = pop_lookup.get(key)
    if total_pop is None or pd.isna(total_pop):
        return None  # no population data
    return round(row["value_pct"] / 100 * total_pop)  # returns 0 when value_pct == 0
