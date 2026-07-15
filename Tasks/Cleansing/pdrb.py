import os
import re
import difflib
import pandas as pd


def _build_province_map(dimension_folder):
    """
    Build {province_name_lower: id_province} from dim_province.xlsx.
    Also adds expanded forms (e.g. "kep. riau" → "kepulauan riau") for
    fuzzy matching flexibility.
    The 'Indonesia' aggregate row is mapped to id = 0.
    """
    dim_path = os.path.join(dimension_folder, "dim_province.xlsx")
    if not os.path.exists(dim_path):
        raise FileNotFoundError(f"dim_province.xlsx not found in {dimension_folder}")

    df = pd.read_excel(dim_path)

    province_map = {}
    for _, row in df.iterrows():
        prov_id = int(row["id_province"])
        name = str(row["province"]).strip().lower()
        province_map[name] = prov_id
        # Also add expanded form: "kep. riau" → "kepulauan riau"
        expanded = re.sub(r"\bkep\.\s*", "kepulauan ", name).strip()
        if expanded != name:
            province_map[expanded] = prov_id

    # Indonesia aggregate
    province_map["indonesia"] = 0

    return province_map


def _fuzzy_id(province_name, province_map, threshold=0.8):
    """
    Return id_province for province_name using fuzzy matching.
    Tries exact match first; falls back to difflib with the given threshold.
    Returns None if no match meets the threshold.
    """
    if not province_name or pd.isna(province_name):
        return None

    name_lower = str(province_name).strip().lower()

    # Exact match
    if name_lower in province_map:
        return province_map[name_lower]

    # Fuzzy match (80% similarity threshold)
    candidates = list(province_map.keys())
    matches = difflib.get_close_matches(name_lower, candidates, n=1, cutoff=threshold)
    if matches:
        return province_map[matches[0]]

    return None


def clean(file_path, dimension_folder=None):
    """
    Clean the PDRB (Regional GDP per capita) file.

    id_province is resolved by fuzzy-matching (≥80%) province names
    against the province list in dim_province.xlsx from the Data Dimension folder.
    The 'province' column is dropped from the output.

    Parameters
    ----------
    file_path : str
        Path to the raw PDRB Excel file.
    dimension_folder : str, optional
        Path to the Data Dimension folder containing dim_province.xlsx.
        If not provided, defaults to 'Data Dimension' relative to the project root.

    Output:
        Columns: id_province, pdrb
    """

    df = pd.read_excel(file_path, dtype=str)

    # Rename columns
    df = df.rename(columns={
        "Provinsi": "province",
        "Produk Domestik Regional Bruto per Kapita Atas Dasar Harga Berlaku (Ribu Rp)": "pdrb",
    })

    # Drop empty / noise rows
    df = df.dropna(subset=["province", "pdrb"])
    df = df[~df["province"].str.contains(
        r"Catatan|Perbedaan|Sumber|^$", na=False, regex=True
    )]

    # Strip any leading "NN. " prefix (in case the file has it)
    df["province"] = df["province"].str.replace(r"^\d+\.\s*", "", regex=True)
    df.loc[df["province"].str.strip().str.lower() == "indonesia", "province"] = "Indonesia"

    # =========================
    # CLEAN PDRB VALUE
    # =========================
    df["pdrb"] = (
        df["pdrb"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace(".", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )

    # =========================
    # ADD INDONESIA TOTAL ROW
    # =========================
    indonesia_total = pd.DataFrame({
        "province": ["Indonesia"],
        "pdrb": [df["pdrb"].sum()],
    })
    df = df[df["province"].str.lower() != "indonesia"]
    df = pd.concat([df, indonesia_total], ignore_index=True)

    # =========================
    # MAP PROVINCE → id_province  (fuzzy, ≥80%)
    # =========================
    if dimension_folder is None:
        # Default: assume project root is two levels up from the file
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(file_path)))
        dimension_folder = os.path.join(project_root, "Data Dimension")
    province_map = _build_province_map(dimension_folder)

    df["id_province"] = df["province"].apply(
        lambda p: _fuzzy_id(p, province_map, threshold=0.8)
    )

    # Indonesia always gets id_province = 0
    df.loc[df["province"].str.strip().str.lower() == "indonesia", "id_province"] = 0

    df["id_province"] = pd.to_numeric(df["id_province"], errors="coerce").astype("Int64")
    df = df.drop(columns=["province"])

    # =========================
    # FINAL OUTPUT
    # =========================
    df = df[["id_province", "pdrb"]].reset_index(drop=True)

    return df
