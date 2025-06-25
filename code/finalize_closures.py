import pandas as pd
from fuzzywuzzy import process

# ── helper ────────────────────────────────────────────────────────────────
def fuzzy_match_ids(raw_names, ref_names, ref_ids, threshold=80):
    """
    Given a Series of raw_names, a list of candidate ref_names and parallel ref_ids,
    returns a DataFrame with columns: matched_name, matched_id, score.
    """
    matches = raw_names.apply(lambda nm: process.extractOne(nm, ref_names) or ("", 0))
    df = pd.DataFrame(matches.tolist(), columns=["matched_name","score"], index=raw_names.index)
    df["matched_id"] = df["matched_name"].map(dict(zip(ref_names, ref_ids)))

    # drop low‐confidence matches (keep score but null out name & id)
    low = df["score"] < threshold
    df.loc[low, ["matched_name","matched_id"]] = pd.NA

    return df

# ── 1) load your computed closures ────────────────────────────────────────
computed = pd.read_csv("Data/processed/school_closure_years.csv",
                       dtype={"SCHOOL_ID": float})
# ensure columns
assert set(computed.columns) >= {"SCHOOL_ID","SCHOOL_NM","GRADE_CAT","closure_year","last_open_year"}

# ── 2) add address‐based 2013 cl osures (if any new) ──────────────────────
addr = pd.read_csv("Data/processed/school_closures_2013_address_review.csv",
                   dtype={"matched_SCHOOL_ID": float})
addr = (
    addr[addr["addr_match_score"]>=90]    # only high-confidence
    [["matched_SCHOOL_ID","matched_SCHOOL_NM","matched_GRADE_CAT","addr_match_score"]]
    .drop_duplicates("matched_SCHOOL_ID")
    .rename(columns={
       "matched_SCHOOL_ID":"SCHOOL_ID",
       "matched_SCHOOL_NM":"SCHOOL_NM",
       "matched_GRADE_CAT":"GRADE_CAT"
    })
)
addr["last_open_year"] = 2012
addr["closure_year"]   = 2013

# pick those not already in computed
to_add_addr = addr[~addr["SCHOOL_ID"].isin(computed["SCHOOL_ID"])]
print(f"Adding {len(to_add_addr)} address-based closures…")

# ── 3) add external reference closures ───────────────────────────────────
ref = pd.read_csv("Data/processed/school_closure_reference.csv",
                  usecols=["SCHOOL_NM","GRADE_CAT","year_closed"])\
         .rename(columns={"year_closed":"closure_year"})

# load shapes to map names→ids
shapes = (
    pd.read_parquet("Data/processed/school_shapes.parquet", engine="fastparquet")
      [["SCHOOL_ID","SCHOOL_NM","GRADE_CAT"]]
      .drop_duplicates(["SCHOOL_ID"])
)
ref_match = fuzzy_match_ids(
    ref["SCHOOL_NM"],
    shapes["SCHOOL_NM"].tolist(),
    shapes["SCHOOL_ID"].tolist(),
    threshold=80
)
ref = pd.concat([ref, ref_match], axis=1)

# only keep successful
ref = ref.dropna(subset=["matched_id"])
ref = ref.rename(columns={
    "matched_id":"SCHOOL_ID",
    "matched_name":"SCHOOL_NM"
})
ref["last_open_year"] = ref["closure_year"] - 1

# save ref dataframe for manual review
ref.to_csv("Data/processed/school_closure_reference_matched.csv", index=False)

#After manual review, re-reading the reference file
ref = pd.read_csv("Data/processed/school_closure_reference_matched.csv",
                  dtype={"SCHOOL_ID": float})

# Compare closure year of School iD in the reference file with computed
closure_review = computed.merge(
    ref[["SCHOOL_ID", "closure_year"]],
    on="SCHOOL_ID",
    how="left",
    suffixes=("", "_ref")
)
# only show those where the closure year differs and a reference exists
closure_diff = closure_review[closure_review["closure_year"] != closure_review["closure_year_ref"]]
closure_diff = closure_diff[closure_diff["closure_year_ref"].notna()]

# keep only those where the closure year matches or closure_year_ref is NaN
# (meaning no reference closure year was found)     
closure_matched = closure_review[
    (closure_review["closure_year"] == closure_review["closure_year_ref"]) |
    (closure_review["closure_year_ref"].isna())
]

# save closure differences for review
closure_diff.to_csv("Data/processed/school_closure_years_review.csv", index=False)

# read reviewed closure_diff, and add to closure_matched
closure_diff_reviewed = pd.read_csv("Data/processed/school_closure_years_reviewed.csv",
                                    dtype={"SCHOOL_ID": float})
# merge reviewed closure_diff with closure_matched
closure_matched = pd.concat([closure_matched, closure_diff_reviewed], ignore_index=True)

# save the closure_matched DataFrame
closure_matched.to_csv("Data/processed/school_closure_years_final.csv", index=False)

# avoid IDs we already have
to_add_ref = ref[~ref["SCHOOL_ID"].isin(computed["SCHOOL_ID"])]
print(f"Adding {len(to_add_ref)} reference-based closures…")

# keep only the canonical closure columns in each DataFrame
BASE_COLS = ["SCHOOL_ID","SCHOOL_NM","GRADE_CAT","last_open_year","closure_year"]

computed = computed[BASE_COLS]
to_add_ref = to_add_ref[BASE_COLS]

# make absolutely sure there are no duplicate column names
computed = computed.loc[:, ~computed.columns.duplicated()]
to_add_ref = to_add_ref.loc[:, ~to_add_ref.columns.duplicated()]

# check for duplicate columns
for df,name in [(computed,"computed"),(to_add_ref,"to_add_ref")]:
    dups = df.columns[df.columns.duplicated()].unique().tolist()
    if dups:
        print(f"{name} has duplicated columns: {dups}")

# now the concat will work
all_close = pd.concat(
    [computed, to_add_ref],
    ignore_index=True,
    sort=False
)

# de-dupe by earliest closure_year
all_close = (
    all_close
    .sort_values("closure_year")
    .drop_duplicates("SCHOOL_ID", keep="first")
    .reset_index(drop=True)
)

# ── 5) save & done ───────────────────────────────────────────────────────
out = "Data/processed/school_closure_years_final.csv"
all_close.to_csv(out, index=False)
print(f"Saved final closure list → {out} ({len(all_close)} schools)")