"""
school_opening_map.py

Processes school data to:
- Create a wide-format table indicating which years each school and grade category was open.
- Generate a table of school closure years.

Inputs:
    Data/processed/school_shapes.parquet : Parquet file with columns:
        - SCHOOL_ID: Unique school identifier
        - GRADE_CAT: Grade category
        - file_year: 4-digit year code as string
        - SCHOOL_NM: School name

Outputs:
    Data/processed/school_open_years.csv      : Wide table of open years per school/grade.
    Data/processed/school_closure_years.csv   : Table of closure years per school/grade.

Assumptions:
    - 'file_year' is a string with a 4-digit year code.
    - Each row represents a school-year-grade combination.
"""

import pandas as pd
import fastparquet
import pyarrow as pa
from fuzzywuzzy import process

# ── 1) Read in processed schools data ──────────────────────────────
schools_df = pd.read_parquet(
    "Data/processed/school_shapes.parquet",
    engine="fastparquet"
)

# ── 2) Derive academic_year_start from file_year ──────────────────
schools_df["academic_year_start"] = (
    schools_df["file_year"].str[:2].astype(int) + 2000
)

# ── 3) Build unique school-grade-year combinations ────────────────
school_year_df = (
    schools_df[["SCHOOL_ID", "GRADE_CAT", "academic_year_start"]]
    .drop_duplicates()
)

# ── 4) Mark each school-year as open ──────────────────────────────
school_year_df["open_dummy"] = 1

# ── 5) Pivot to wide format: one row per school/grade, columns for each year ─
school_open_wide = (
    school_year_df
    .pivot_table(
        index=["SCHOOL_ID", "GRADE_CAT"],
        columns="academic_year_start",
        values="open_dummy",
        aggfunc="max",
        fill_value=0
    )
    .reset_index()
)

# ── 6) Merge in representative school name ────────────────────────
first_names = (
    schools_df
    .sort_values("file_year")
    .drop_duplicates(subset="SCHOOL_ID")[["SCHOOL_ID", "SCHOOL_NM"]]
)
school_open_wide = school_open_wide.merge(first_names, on="SCHOOL_ID", how="left")

# ── 7) Rename year-columns for clarity (e.g., 2015 -> open_2015) ──
school_open_wide.columns.name = None
year_cols = [c for c in school_open_wide.columns if isinstance(c, int)]
school_open_wide = school_open_wide.rename(
    columns={yr: f"open_{yr}" for yr in year_cols}
)

# ── 8) Sort and reorder columns for readability ───────────────────
school_open_wide = school_open_wide.sort_values(
    by=["SCHOOL_ID", "GRADE_CAT"]
).reset_index(drop=True)
school_open_wide = school_open_wide[["SCHOOL_ID", "SCHOOL_NM", "GRADE_CAT"] +
    [col for col in school_open_wide.columns if col not in ["SCHOOL_ID", "SCHOOL_NM", "GRADE_CAT"]]
]

# ── 9) Build long table of school-year and compute closure years ──
school_years = (
    schools_df[["SCHOOL_ID", "SCHOOL_NM", "GRADE_CAT", "academic_year_start"]]
    .drop_duplicates()
)
last_open = (
    school_years
    .groupby(["SCHOOL_ID", "GRADE_CAT"], as_index=False)
    .agg(last_open_year=("academic_year_start", "max"))
)
last_open["closure_year"] = last_open["last_open_year"] + 1
rep_names = (
    schools_df.sort_values("file_year")
    .drop_duplicates("SCHOOL_ID")[["SCHOOL_ID", "SCHOOL_NM"]]
)
last_open = last_open.merge(rep_names, on="SCHOOL_ID", how="left")
last_open = last_open[last_open["closure_year"] < 2019]
last_open = last_open[["SCHOOL_ID", "SCHOOL_NM", "GRADE_CAT"] +
    [col for col in last_open.columns if col not in ["SCHOOL_ID", "SCHOOL_NM", "GRADE_CAT"]]
]

# ── 10) Save outputs ─────────────────────────────────────────────
print(school_open_wide.head())
school_open_wide.to_csv("Data/processed/school_open_years.csv", index=False)

print(last_open.head())
last_open.to_csv("Data/processed/school_closure_years.csv", index=False)

# ── 11) Load raw 2013 closures and match names ────────────────────
raw_closures = pd.read_csv("Data/raw_data/school_closures_2013.csv")

# Detect which column holds the school name (here it's "School")
raw_name_col = "School" if "School" in raw_closures.columns else raw_closures.columns[0]
raw_closures["raw_school_name"] = raw_closures[raw_name_col].astype(str)

# Prepare reference list of computed school names
ref_names = last_open["SCHOOL_NM"].dropna().unique().tolist()

# Fuzzy‐match each raw name to the best candidate in ref_names
matches = raw_closures["raw_school_name"].apply(
    lambda nm: process.extractOne(nm, ref_names) or ("", 0)
)
raw_closures[["matched_name", "match_score"]] = pd.DataFrame(
    matches.tolist(), index=raw_closures.index
)

# Save a summary of match results
raw_closures.to_csv(
    "Data/processed/school_closures_2013_matched.csv", index=False
)
print(raw_closures[["raw_school_name", "matched_name", "match_score"]].head())

# ── 12) Review raw_closures with a match_score < 100 and get top 3 candidates ──
# Load all shape school names and IDs for reference
shapes_full = pd.read_parquet(
    "Data/processed/school_shapes.parquet",
    engine="fastparquet"
)[["SCHOOL_NM", "SCHOOL_ID"]].dropna()
name_id_map = dict(zip(shapes_full["SCHOOL_NM"], shapes_full["SCHOOL_ID"]))

# Filter to those with imperfect matches
unmatched = raw_closures[raw_closures["match_score"] < 100].copy()

# Compute top 3 fuzzy matches against all shape names
unmatched["top_matches"] = unmatched["raw_school_name"].apply(
    lambda nm: process.extract(nm, shapes_full["SCHOOL_NM"].tolist(), limit=3)
)

# Expand the top 3 into separate columns
top3 = pd.DataFrame(
    unmatched["top_matches"].tolist(),
    columns=["m1", "m2", "m3"],
    index=unmatched.index
)
for i in (1, 2, 3):
    unmatched[f"match{i}"], unmatched[f"score{i}"] = zip(*top3[f"m{i}"])

# Map matched names to their SCHOOL_ID
unmatched["match1_id"] = unmatched["match1"].map(name_id_map)
unmatched["match2_id"] = unmatched["match2"].map(name_id_map)
unmatched["match3_id"] = unmatched["match3"].map(name_id_map)

# Prepare review DataFrame including IDs for top 3 matches
review_df = unmatched[ [
    "raw_school_name", "matched_name", "match_score",
    "match1", "score1", "match1_id",
    "match2", "score2", "match2_id",
    "match3", "score3", "match3_id"
]]

# Save results for manual review
review_df.to_csv(
    "Data/processed/school_closures_2013_review_matches.csv",
    index=False
)
print("Review files saved for manual inspection of unmatched closures.")

# ── 12) Review raw_closures with a match_score < 100 and get top 3 candidates ──
# Load all shape school names and IDs for reference
shapes_full = pd.read_parquet(
    "Data/processed/school_shapes.parquet",
    engine="fastparquet"
)[["SCHOOL_NM", "SCHOOL_ID"]].dropna()
name_id_map = dict(zip(shapes_full["SCHOOL_NM"], shapes_full["SCHOOL_ID"]))

# Filter to those with imperfect matches
unmatched = raw_closures[raw_closures["match_score"] < 100].copy()

# Compute top 3 fuzzy matches against all shape names
unmatched["top_matches"] = unmatched["raw_school_name"].apply(
    lambda nm: process.extract(nm, shapes_full["SCHOOL_NM"].tolist(), limit=3)
)

# Expand the top 3 into separate columns
top3 = pd.DataFrame(
    unmatched["top_matches"].tolist(),
    columns=["m1", "m2", "m3"],
    index=unmatched.index
)
for i in (1, 2, 3):
    unmatched[f"match{i}"], unmatched[f"score{i}"] = zip(*top3[f"m{i}"])

# Map matched names to their SCHOOL_ID
unmatched["match1_id"] = unmatched["match1"].map(name_id_map)
unmatched["match2_id"] = unmatched["match2"].map(name_id_map)
unmatched["match3_id"] = unmatched["match3"].map(name_id_map)

# Prepare review DataFrame including IDs for top 3 matches
review_df = unmatched[ [
    "raw_school_name", "matched_name", "match_score",
    "match1", "score1", "match1_id",
    "match2", "score2", "match2_id",
    "match3", "score3", "match3_id"
]]

# Save results for manual review
review_df.to_csv(
    "Data/processed/school_closures_2013_review_matches.csv",
    index=False
)

print("Review files saved for manual inspection of unmatched closures.")

# ── 13) Address‐based review: match raw_closures address to school_shapes addresses ──
# (Run this after the fuzzy‐name review above)

# Load shape addresses
shapes_df = pd.read_parquet("Data/processed/school_shapes.parquet", engine="fastparquet")
addr_candidates = shapes_df["SCHOOL_ADD"].dropna().unique().tolist()

# Identify raw address column in raw_closures
# e.g. "Address" or any column containing "addr"
possible = [c for c in raw_closures.columns if "addr" in c.lower()]
raw_addr_col = possible[0] if possible else None
if raw_addr_col is None:
    raise KeyError(f"Could not find an address column in raw_closures; columns: {raw_closures.columns.tolist()}")

raw_closures["raw_address"] = raw_closures[raw_addr_col].astype(str)

# Fuzzy‐match each raw_address to the best candidate in addr_candidates
from fuzzywuzzy import process
addr_matches = raw_closures["raw_address"].apply(
    lambda a: process.extractOne(a, addr_candidates) or ("", 0)
)
raw_closures[["matched_address", "addr_match_score"]] = pd.DataFrame(
    addr_matches.tolist(), index=raw_closures.index
)

# Map matched_address back to SCHOOL_ID, SCHOOL_NM and GRADE_CAT in shapes_df
addr_map = (
    shapes_df[["SCHOOL_ADD", "SCHOOL_ID", "SCHOOL_NM", "GRADE_CAT"]]
    .dropna(subset=["SCHOOL_ADD"])
    .drop_duplicates("SCHOOL_ADD")
    .rename(columns={
        "SCHOOL_ADD": "matched_address",
        "SCHOOL_ID": "matched_SCHOOL_ID",
        "SCHOOL_NM": "matched_SCHOOL_NM",
        "GRADE_CAT": "matched_GRADE_CAT"
    })
)
address_review = raw_closures.merge(addr_map, on="matched_address", how="left")

# Select and save the review table with added name and grade category
address_review = address_review[[
    "raw_school_name",    # include the original raw school name
    "raw_address",
    "matched_address",
    "matched_SCHOOL_ID",
    "matched_SCHOOL_NM",
    "matched_GRADE_CAT",
    "addr_match_score"
]]
address_review.to_csv(
    "Data/processed/school_closures_2013_address_review.csv",
    index=False
)

print("Address review file saved for manual inspection of unmatched closures.")



