import pandas as pd

NUMBER_GROUPS = {"cps", "bts", "ifc"}


def convert_number_to_percentage(combined_data):
    """
    For number-based groups (cps, bts, ifc):
    - Rename 'value' -> 'value_num'
    - Add 'value_pct' = value_num / nov * 100
      where nov = number_of_villages matched by (id_province, area_category, year)
    """
    nov_df = combined_data.get("nov")
    if nov_df is None:
        raise KeyError("'nov' not found in combined_data — number of villages data is required")

    nov_lookup = (
        nov_df[["id_province", "area_category", "year", "value"]]
        .set_index(["id_province", "area_category", "year"])["value"]
        .to_dict()
    )

    result = {}

    for group_key, df in combined_data.items():
        if group_key not in NUMBER_GROUPS:
            result[group_key] = df
            continue

        df = df.copy()
        df = df.rename(columns={"value": "value_num"})

        df["value_pct"] = df.apply(
            lambda row: _calc_pct(row, nov_lookup),
            axis=1,
        )

        print(f"  [{group_key}] number -> percentage conversion applied (per nov)")
        result[group_key] = df

    return result


def _calc_pct(row, nov_lookup):
    if pd.isna(row["value_num"]):
        return None  # no data
    nov = nov_lookup.get((row["id_province"], row["area_category"], row["year"]))
    if nov is None or pd.isna(nov) or nov == 0:
        return None  # no nov data
    return round(row["value_num"] / nov * 100, 2)  # returns 0.0 when value_num == 0
