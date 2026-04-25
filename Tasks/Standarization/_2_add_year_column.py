import pandas as pd
import os
import re

FILES_WITH_YEAR_IN_DATA = {"ai.xlsx", "ai_rural.xlsx", "ai_urban.xlsx"}


def add_year_from_env(df, year):
    df["year"] = int(year)
    return df


def extract_year_from_variable(df):
    df = df.rename(columns={"variable": "year"})
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df


# =========================
# 🔥 NEW FUNCTION: extract year from filename
# =========================
def extract_year_from_filename(filename):
    match = re.search(r"(\d{4})", filename)
    return int(match.group(1)) if match else None


def add_year_column(df, filename, year_env):

    filename_lower = filename.lower()

    if filename_lower in FILES_WITH_YEAR_IN_DATA:
        return extract_year_from_variable(df)

    if ("pdrb" in filename_lower) or ("penduduk" in filename_lower):
        year = extract_year_from_filename(filename)

        if year is None:
            raise ValueError(f"Year not found in filename: {filename}")

        return add_year_from_env(df, year)

    return add_year_from_env(df, year_env)