from os.path import exists
from os import makedirs
import time
import pandas as pd
import re


# -----------------------------
# ISBN CLEANING
# -----------------------------
def clean_isbn(val):
    """Extract a clean 13-digit or ISBN-10 value. Prefer 13-digit."""
    if pd.isna(val):
        return None

    val = str(val).strip()

    # Prefer 13-digit first
    match13 = re.search(r"\b\d{13}\b", val)
    if match13:
        return match13.group(0)

    # Then ISBN-10
    match10 = re.search(r"\b\d{9}[\dXx]\b", val)
    if match10:
        return match10.group(0)

    return None


# -----------------------------
# PROCESS CSV
# -----------------------------
def process(from_file, semester_year):
    """Load CSV, clean ISBNs, filter for semester, return list + DataFrame."""

    if not exists(from_file):
        print("File not found:", from_file)
        return None, None

    print(f"Opening {from_file}")

    # These match the *actual* extract format you showed
    column_names = [
        "HEGIS_Code",
        "Course_No",
        "field_section",
        "Unknown",
        "field_instructor",
        "title",
        "field_author",
        "Edition",
        "ISBN",
        "Status",
        "Unknown_ID",
        "Semester_Year",
        "New Purchase Price",
        "Used Purchase Price",
        "New Rental Price",
        "Used Rental Price",
        "Digital License Term (Days)"
    ]

    try:
        indata = pd.read_csv(
            from_file,
            index_col=False,
            header=None,
            names=column_names,
            usecols=range(len(column_names)),
            dtype=str,
            encoding="utf-8",
            encoding_errors="ignore"
        )
    except Exception as e:
        print("Error reading CSV:", e)
        return None, None

    # Normalize semester values
    indata["Semester_Year"] = (
        indata["Semester_Year"]
        .astype(str)
        .str.encode("ascii", "ignore")
        .str.decode("utf-8")
        .str.strip()
        .str.upper()
    )

    print("🔎 DEBUG: Semesters seen BEFORE filtering:", indata["Semester_Year"].unique())

    # Remove garbage rows (seen in your extract)
    indata = indata[indata["HEGIS_Code"].astype(str).str.upper() != "XTRA"]

    # Remove rows missing core data
    indata = indata.dropna(
        subset=["HEGIS_Code", "Course_No", "ISBN", "Semester_Year"]
    )

    # Filter by requested semester
    indata = indata[indata["Semester_Year"] == semester_year.upper()]
    print(f"DEBUG: Rows after filtering '{semester_year}': {len(indata)}")

    # Clean ISBN values
    indata["ISBN"] = indata["ISBN"].astype(str).str.strip()
    indata["ISBN"] = indata["ISBN"].apply(clean_isbn)
    indata = indata.dropna(subset=["ISBN"])

    unique_isbn = indata["ISBN"].drop_duplicates().tolist()
    print("DEBUG Sample ISBNs:", unique_isbn[:5])
    print("Total unique ISBN:", len(unique_isbn))

    return unique_isbn, indata

def close(indata):
    """
    Normalize final DataFrame for output:
    - Insert field_course (HEGIS + Course_No)
    - Drop raw source columns
    - Sort by PRIMO_Total_Results
    - Save CSV
    """

    # Build field_course like old output
    indata["field_course"] = (
        indata["HEGIS_Code"].astype(str).str.strip()
        + " "
        + indata["Course_No"].astype(str).str.strip()
    )
    # Move to column 0
    first = indata.pop("field_course")
    indata.insert(0, "field_course", first)

    # Old output removed these
    for col in ["HEGIS_Code", "Course_No", "Unknown"]:
        if col in indata.columns:
            indata.drop(columns=col, inplace=True)

    # Ensure folder exists
    if not exists("data"):
        makedirs("data")

    # Filename
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    out_file = f"data/output_file_{timestamp}.csv"

    # Sort output
    sort_by = "PRIMO_Total_Results" if "PRIMO_Total_Results" in indata.columns else indata.columns[0]

    indata_sorted = indata.sort_values(by=sort_by, ascending=False)

    # Save output
    indata_sorted.to_csv(out_file, index=False)
    print(f"💾 Saved output to: {out_file}")

    return out_file
