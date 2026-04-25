import pandas as pd


def clean(file_path):
    """
    Clean the total population (Total Penduduk) file.
    """

    df = pd.read_excel(file_path, header=None, dtype=str)

    # =========================
    # FIX 1: KEEP TOTAL ROW (IMPORTANT)
    # =========================
    df = df[df[0].str.match(r"^\d+\.|^TOTAL$", na=False)].reset_index(drop=True)

    # Assign standard column names
    col_map = {
        df.columns[0]: "province_raw",
        df.columns[1]: "urban_m",
        df.columns[2]: "urban_f",
        df.columns[3]: "urban",
        df.columns[4]: "rural_m",
        df.columns[5]: "rural_f",
        df.columns[6]: "rural",
        df.columns[7]: "all_area_m",
        df.columns[8]: "all_area_f",
    }
    df = df.rename(columns=col_map)

    # =========================
    # FIX 2: TOTAL -> INDONESIA
    # =========================
    df["province_raw"] = df["province_raw"].replace(
        r"(?i)^total$", "Indonesia", regex=True
    )

    # Extract numeric province ID
    df["id_province"] = df["province_raw"].astype(str).str.extract(r"^(\d+)")

    # Set Indonesia = 0
    df.loc[
        df["province_raw"].str.strip().str.lower() == "indonesia",
        "id_province"
    ] = "0"

    df["id_province"] = pd.to_numeric(df["id_province"], errors="coerce").astype("Int64")

    # =========================
    # CONVERT NUMERIC COLUMNS
    # =========================
    for col in ["urban", "rural", "all_area_m", "all_area_f"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace(".", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
        )

    # Derive total
    df["all_area"] = df["all_area_m"] + df["all_area_f"]

    # =========================
    # FINAL OUTPUT
    # =========================
    df = df[["id_province", "urban", "rural", "all_area"]].reset_index(drop=True)

    return df