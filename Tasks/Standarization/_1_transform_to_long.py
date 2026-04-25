import pandas as pd


# List file name with no indicator column (e.g. variable name is in the column header, not in the value)
def get_indicator_from_filename(filename):
    name = filename.replace(".xlsx", "").lower()

    if name in {"ai", "ai_rural", "ai_urban"}:
        return "internet_users"
    
    if name.startswith("total_penduduk"):
        return "total_population"
    
    if name.startswith("nov"):
        return "number_of_villages"

    return name

def is_no_indicator_file(filename):
    name = filename.replace(".xlsx", "").lower()

    return (
        name in {"ai", "ai_rural", "ai_urban"}
        or name.startswith("total_")
        or name.startswith("nov")
    )


def transform_no_indicator(df, filename):
    df = df.melt(
        id_vars=["id_province"],
        var_name="variable",   
        value_name="value"
    )

    df["id_indicator"] = get_indicator_from_filename(filename)

    return df


def transform_with_indicator(df):
    df = df.melt(
        id_vars=["id_province"],
        var_name="id_indicator",
        value_name="value"
    )

    return df


def transform_to_long(df, filename):
    if is_no_indicator_file(filename):
        return transform_no_indicator(df, filename)
    else:
        return transform_with_indicator(df)