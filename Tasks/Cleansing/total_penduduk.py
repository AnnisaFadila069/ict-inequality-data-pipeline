import pandas as pd


def clean(file_path):
    """
    Clean the total population (Total Penduduk) file.
    """

    df = pd.read_excel(file_path, header=None, dtype=str)

    # =========================
    # FILTER VALID ROWS & SELECT COLUMNS
    # =========================
    # Keep only province rows ("NN.") and the national total ("TOTAL")
    df = df[df[0].str.match(r"^\d+\.|^TOTAL$", na=False)].reset_index(drop=True)

    # Select only the needed columns: province_raw(0), urban(3), rural(6), all_area(9)
    df = df[[0, 3, 6, 9]]
    df.columns = ["province_raw", "urban", "rural", "all_area"]

    # =========================
    # REPLACE "TOTAL" → "Indonesia"
    # =========================
    df["province_raw"] = df["province_raw"].replace(
        r"(?i)^total$", "Indonesia", regex=True
    )

    # =========================
    # EXTRACT id_province
    # =========================
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
    for col in ["urban", "rural", "all_area"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace(".", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
        )

    # =========================
    # FINAL OUTPUT
    # =========================
    df = df[["id_province", "urban", "rural", "all_area"]].reset_index(drop=True)

    return df
