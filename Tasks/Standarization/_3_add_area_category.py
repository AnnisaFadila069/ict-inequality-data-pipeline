import pandas as pd

FILES_WITH_AREA_CLASSIFICATION = {"nov.xlsx"}


def is_area_classification_file(filename):
    """
    Detect files that should use column-based area:
    - nov.xlsx
    - total_*
    """
    name = filename.lower()

    return (
        name in FILES_WITH_AREA_CLASSIFICATION
        or name.startswith("total_")
    )


def get_area_category(filename):
    name = filename.lower()

    if "urban" in name:
        return "urban"
    elif "rural" in name:
        return "rural"
    else:
        return "all_area"


def normalize_area_category(series):
    """
    Ensure only valid values:
    urban / rural / else → all_area
    """
    return (
        series.astype(str)
        .str.lower()
        .str.strip()
        .apply(lambda x: x if x in ["urban", "rural"] else "all_area")
    )


def transform_area_classification(df):
    """
    Handle IF & TOTAL files:
    - If already long → rename variable → area_category
    - If still wide → melt first
    """

    # ✅ CASE 1: already long
    if "variable" in df.columns and "value" in df.columns:
        df = df.rename(columns={"variable": "area_category"})

    # ✅ CASE 2: still wide
    else:
        df = df.melt(
            id_vars=["id_province"],
            var_name="area_category",
            value_name="value"
        )

    # 🔥 VALIDATION (centralized)
    df["area_category"] = normalize_area_category(df["area_category"])

    return df


def add_area_category(df, filename):

    filename = filename.lower()

    # ✅ SPECIAL CASE: IF + TOTAL*
    if is_area_classification_file(filename):
        return transform_area_classification(df)

    # ✅ DEFAULT CASE
    df["area_category"] = get_area_category(filename)

    return df