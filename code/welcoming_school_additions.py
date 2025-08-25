import geopandas as gpd
import pandas as pd
import folium
from shapely.geometry import mapping
from fuzzywuzzy import process


# 1) Load existing closures
closure_df = pd.read_csv("Data/processed/school_closure_years.csv",
                         dtype={"SCHOOL_ID": float})

# 2) Load raw 2013‐wave closures
raw_2013 = pd.read_csv("Data/raw_data/school_closures_2013.csv",
                       dtype={"SCHOOL_ID": float})
# 3) Load in school shapes, filter to just 2012–13 & 2013–14, and drop geometry
schools_shapes = (
    gpd
    .read_parquet("Data/processed/school_shapes.parquet")
    .query("file_year in ['1213','1314']")
)

# Remove the GeoDataFrame’s geometry column so it becomes a normal DataFrame
if "geometry" in schools_shapes.columns:
    schools_shapes = schools_shapes.drop(columns=["geometry"])

# Now schools_shapes is a pandas DataFrame with only the attributes for years '1213' &

print(schools_shapes.head())

# 4) Match "School" in raw_2013 to shapes by fuzzy matching "School" with "SCHOOL_NM" assuming SCHOOL_ID 
#    is the missing from raw_2013
def match_school_names(raw_df: pd.DataFrame,
                       shapes_df: pd.DataFrame,
                       threshold: int = 90) -> pd.DataFrame:
    """
    Fuzzy‐match raw_df SCHOOL_NM to shapes_df SCHOOL_NM,
    only assigning SCHOOL_ID and GRADE_CAT when match score >= threshold.
    """
    # prepare lists of candidate names
    shape_names = shapes_df["SCHOOL_NM"].tolist()

    # ensure columns exist
    raw_df["matched_name"] = None
    raw_df["match_score"] = 0
    raw_df["SCHOOL_ID"] = pd.NA
    raw_df["GRADE_CAT"] = pd.NA

    for idx, row in raw_df.iterrows():
        name = row["School"]
        match_name, score = process.extractOne(name, shape_names) or ("", 0)
        raw_df.at[idx, "matched_name"] = match_name
        raw_df.at[idx, "match_score"] = score
        if score >= threshold:
            # lookup SCHOOL_ID and GRADE_CAT from shapes_df
            match_row = shapes_df[shapes_df["SCHOOL_NM"] == match_name].iloc[0]
            raw_df.at[idx, "SCHOOL_ID"] = match_row["SCHOOL_ID"]
            raw_df.at[idx, "GRADE_CAT"] = match_row["GRADE_CAT"]
    return raw_df

# … later in the script …
raw_2013 = match_school_names(raw_2013, schools_shapes, threshold=90)


# 3) Find which IDs are missing
missing = raw_2013.loc[~raw_2013.SCHOOL_ID.isin(closure_df.SCHOOL_ID)]
print(f"Schools in raw 2013 list not in processed: {len(missing)}")
print(missing)

# 4) Build new rows for those missing
#    Assume raw has SCHOOL_ID, SCHOOL_NM, GRADE_CAT
new_rows = missing.assign(
    last_open_year=2013,
    closure_year=2014
)[["SCHOOL_ID", "SCHOOL_NM", "GRADE_CAT", "last_open_year", "closure_year"]]

# 5) Append and save back
updated = pd.concat([closure_df, new_rows], ignore_index=True)
updated.to_csv("Data/processed/school_closure_years.csv", index=False)
print("Appended missing schools and saved updated school_closure_years.csv")

# 2) load your school shapes in a metric CRS for area calcs
schools_3857 = (
    gpd.read_parquet("Data/processed/school_shapes.parquet")
    .to_crs(epsg=3857)
)
schools_3857["academic_year_start"] = (
    schools_3857["file_year"].str[:2].astype(int) + 2000
)

# 3) reproject to WGS84 (4326) for plotting with Folium
schools_map_gdf = schools_3857.to_crs(epsg=4326)

def calculate_area_transfer(prev_year_gdf, next_year_gdf, closed_school_ids):
    # Extract closed school boundaries from previous year
    closed_schools = prev_year_gdf[prev_year_gdf["SCHOOL_ID"].isin(closed_school_ids)]
    
    # Extract open school boundaries from next year
    open_schools = next_year_gdf[~next_year_gdf["SCHOOL_ID"].isin(closed_school_ids)]
    
    if closed_schools.empty or open_schools.empty:
        return pd.DataFrame()  # No transfers possible
    
    # Spatial intersection to find transferred areas
    intersection = gpd.overlay(closed_schools, open_schools, how='intersection')
    intersection["transferred_area_sqm"] = intersection.geometry.area
    
    # Summarize transferred areas
    summary = intersection.groupby(
        ["SCHOOL_ID_1", "SCHOOL_NM_1", "SCHOOL_ID_2", "SCHOOL_NM_2"]
    )["transferred_area_sqm"].sum().reset_index()
    
    summary.rename(columns={
        "SCHOOL_ID_1": "Closed_SCHOOL_ID",
        "SCHOOL_NM_1": "Closed_SCHOOL_NM",
        "SCHOOL_ID_2": "Receiving_SCHOOL_ID",
        "SCHOOL_NM_2": "Receiving_SCHOOL_NM"
    }, inplace=True)
    
    return summary

years = range(2008, 2019)
all_transfers = []

for year in years:
    prev_year_gdf = schools_3857[schools_3857["academic_year_start"] == year]
    next_year_gdf = schools_3857[schools_3857["academic_year_start"] == year + 1]
    
    # Identify schools closed at the end of the current year
    closed_school_ids = closure_df[closure_df["closure_year"] == year]["SCHOOL_ID"].tolist()
    
    # Calculate area transfers
    transfer_df = calculate_area_transfer(prev_year_gdf, next_year_gdf, closed_school_ids)
    
    if not transfer_df.empty:
        transfer_df["closure_year"] = year
        all_transfers.append(transfer_df)

# Combine all yearly transfers into a single DataFrame
final_transfer_df = pd.concat(all_transfers, ignore_index=True)

final_transfer_df.to_csv("Data/processed/school_area_transfers.csv", index=False)