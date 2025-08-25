import pandas as pd
from fuzzywuzzy import process

# 1) Read the address‐based review of 2013 closures
addr_review = pd.read_csv(
    "Data/processed/school_closures_2013_address_review.csv",
    dtype={"matched_SCHOOL_ID": float}
)

# Extract unique matched schools (ID, name, grade)
addr_unique = (
    addr_review[["matched_SCHOOL_ID", "matched_SCHOOL_NM", "matched_GRADE_CAT"]]
    .dropna(subset=["matched_SCHOOL_ID"])
    .drop_duplicates(subset=["matched_SCHOOL_ID"])
    .rename(columns={
        "matched_SCHOOL_ID": "SCHOOL_ID",
        "matched_SCHOOL_NM": "SCHOOL_NM",
        "matched_GRADE_CAT": "GRADE_CAT"
    })
)

# 2) Load existing closure records
closure_df = pd.read_csv(
    "Data/processed/school_closure_years.csv",
    dtype={"SCHOOL_ID": float}
)

# 3) Identify which matched IDs are not yet in closure_df
to_add = addr_unique[~addr_unique["SCHOOL_ID"].isin(closure_df["SCHOOL_ID"])]
print(f"Appending {len(to_add)} new closures from address review:")
print(to_add)

if not to_add.empty:
    # 4) Assign last_open_year and closure_year for these 2013 closures
    to_add = to_add.assign(
        last_open_year=2012,
        closure_year=2013
    )

    # 5) Append and save closure_df closure list
    closure_df = pd.concat([closure_df, to_add], ignore_index=True)
else:
    print("No new closures")

# Save the closure_df closure records
closure_df.to_csv("Data/processed/school_closure_years.csv", index=False)
print("closure_df school_closure_years.csv with address‐based closures.")

# 6) Read the closure reference file
ref_df = pd.read_csv(
    "Data/processed/school_closure_reference.csv",
    usecols=["SCHOOL_NM","GRADE_CAT","year_closed"],
) .rename(columns={
        "year_closed": "closure_year"
    })

# 7) Load school_shapes names and IDs for matching
shapes = (
    pd.read_parquet("Data/processed/school_shapes.parquet", engine="fastparquet")
      [["SCHOOL_ID", "SCHOOL_NM"]]
      .dropna(subset=["SCHOOL_NM"])
      .drop_duplicates()
)
name_to_id = dict(zip(shapes["SCHOOL_NM"], shapes["SCHOOL_ID"]))
shape_names = shapes["SCHOOL_NM"].tolist()

# 8) Fuzzy‐match each reference name to the best SCHOOL_NM in shapes
matches = ref_df["SCHOOL_NM"].apply(lambda nm: process.extractOne(nm, shape_names) or ("", 0))
ref_df[["matched_name", "match_score"]] = pd.DataFrame(matches.tolist(), index=ref_df.index)

# 9) Map the matched shape name to its SCHOOL_ID
ref_df["matched_SCHOOL_ID"] = ref_df["matched_name"].map(name_to_id)

# 10) Filter the full closure_df to only those SCHOOL_IDs in the reference
closure_ref_df = closure_df[
    closure_df["SCHOOL_ID"].isin(ref_df["matched_SCHOOL_ID"].dropna().unique())
].copy()

# 12) Review the closure year for the schools in the reference compared to the closure_df
closure_ref_df = closure_ref_df.merge(
    ref_df[["matched_SCHOOL_ID", "closure_year"]],
    left_on="SCHOOL_ID",
    right_on="matched_SCHOOL_ID",
    how="left"
)

# 13) Filter to only those with a closure year match
closure_err_df = closure_ref_df[
    closure_ref_df["closure_year_x"] != closure_ref_df["closure_year_y"]
].drop(columns=["matched_SCHOOL_ID"])





# 11) Optionally save or inspect
closure_ref_df.to_csv(
    "Data/processed/school_closure_years_reference.csv",
    index=False
)
print("Filtered closures to reference list:", closure_ref_df.shape[0])