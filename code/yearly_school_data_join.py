
# Joining yearly reporting for CPS data

import os
import glob
import pandas as pd
import re
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point


# Define the folder where CSV files live:
csv_folder = "Data/temp"

# Define keywords you're interested in:
keywords = ["Attendance","Boundaries"]

# Find matching files:
matching_files = []
for keyword in keywords:
    matched = glob.glob(os.path.join(csv_folder, f"*{keyword}*.csv"))
    matching_files.extend(matched)

# Remove duplicates if any
matching_files = list(set(matching_files))

matching_files = [f for f in matching_files if ('network' not in os.path.basename(f).lower() and 'charter' not in os.path.basename(f).lower())]

print(f"Found {len(matching_files)} files matching Boundaries")

elementary_files = [f for f in matching_files if "elementary" in os.path.basename(f).lower()]
middle_files = [f for f in matching_files if "middle" in os.path.basename(f).lower()]
high_files = [f for f in matching_files if "high" in os.path.basename(f).lower()]

print(f"Files for elementary schools: {len(elementary_files)}")
print(f"Files for middle schools: {len(middle_files)}")
print(f"Files for high schools: {len(high_files)}")

def file_columns_df(file_list):
    data = []
    for filepath in file_list:
        try:
            df = pd.read_csv(filepath, nrows=0)
            data.append({
                "file_path": filepath,
                "column_names": list(df.columns)
            })
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
    return pd.DataFrame(data)

# Look at file columns
file_cols = file_columns_df(matching_files)

# Process high school files: standardize columns, add year column, then merge
dfs = []
for filepath in matching_files:
    try:
        df = pd.read_csv(filepath)
        
        # Rename columns for all files using the provided rename_map:
        rename_map = {
            "BoundaryGr": "BOUNDARYGR",
            "School_NM":  "SCHOOL_NM",
            "SchoolID":   "SCHOOL_ID",
            "Grade_Cat":  "GRADE_CAT",
            "SchoolAddr": "SCHOOL_ADD",
            "SchoolName": "SCHOOL_NM",
            "SCHOOLID": "SCHOOL_ID",
            "SCHOOL_Nam": "SCHOOL_NM"
        }
        df.rename(columns=rename_map, inplace=True)
        
        basename = os.path.basename(filepath)

        # Extract the 4-digit school-year code (e.g. "1516") if present:
        m = re.search(r"SY(\d{4})", basename)
        if m:
            year_code = m.group(1)        # e.g. "1516"
            df["file_year"] = year_code   # keep as string, or int(year_code) if you like
        else:
            # fallback: grab any 4-digit run
            m2 = re.search(r"(\d{4})", basename)
            if m2:
                df["file_year"] = m2.group(1)
            else:
                df["file_year"] = None  # or raise an error

        dfs.append(df)

    except Exception as e:
        print(f"Error processing {filepath}: {e}")

# 3) Concatenate all DataFrames into one:
school_shapes = pd.concat(dfs, ignore_index=True, sort=False)[['SCHOOL_ID', 'SCHOOL_NM', 'the_geom', 'SCHOOL_ADD', 'GRADE_CAT', 'BOUNDARYGR', 'file_year']]
print("Merged shape:", school_shapes.shape)
print("Columns in merged_high:", school_shapes.columns.tolist())

# ────────────────────────────────────────────────────────────────────────────────
# 3A) LOAD YOUR SCHOOL BOUNDARIES AS A GeoDataFrame
# ────────────────────────────────────────────────────────────────────────────────

# 2) Convert the "the_geom" column (WKT) into actual shapely Polygons
#    and build a GeoDataFrame. Assume the WKT is in EPSG:4326 (lat/lon).
school_shapes["geometry"] = school_shapes["the_geom"].apply(wkt.loads)
schools_gdf = gpd.GeoDataFrame(
    school_shapes.drop(columns=["the_geom"]),
    geometry="geometry",
    crs="EPSG:4326"  # adjust if your WKT is in a different CRS
)

# Save the schools GeoDataFrame to a Parquet file:
schools_gdf.to_parquet("Data/processed/school_shapes.parquet", index=False)

# 3) Inspect to make sure it loaded correctly:
print("Total schools:", len(schools_gdf))

# ────────────────────────────────────────────────────────────────────────────────
# 3B) LOAD YOUR CRIME DATA AS POINTS
# ────────────────────────────────────────────────────────────────────────────────

# Suppose you've already filtered crimes to only those at “School” locations
# and have a DataFrame called 'school_crime_df' with columns including 'date', 'latitude', 'longitude'.

school_crime_df = pd.read_parquet("Data/processed/crime_at_schools.parquet")  # or however you loaded it

# 2) Your crime GeoDataFrame, as before:
school_crime_gdf = gpd.GeoDataFrame(
    school_crime_df,
    geometry=gpd.points_from_xy(school_crime_df.longitude, school_crime_df.latitude),
    crs="EPSG:4326"
)

# Reproject both to a projected CRS for the join:
schools_proj = schools_gdf.to_crs(epsg=3857)
crime_proj   = school_crime_gdf.to_crs(epsg=3857)

# 3) Spatial join (point-in-polygon):
joined_many = gpd.sjoin(
    crime_proj,
    schools_proj,
    how="left",
    predicate="within"
).to_crs(epsg=4326)

# 4) Group so each crime collects lists of overlapping school_ids by GRADE_CAT
def collect_by_grade(df):
    return pd.Series({
        "ES_schools": df.loc[df.GRADE_CAT == "ES", "SCHOOL_ID"]
                          .dropna().unique().tolist(),
        "MS_schools": df.loc[df.GRADE_CAT == "MS", "SCHOOL_ID"]
                          .dropna().unique().tolist(),
        "HS_schools": df.loc[df.GRADE_CAT == "HS", "SCHOOL_ID"]
                          .dropna().unique().tolist(),
    })

crime_with_grades = (
    joined_many
    .groupby(
      ["id", "date", "primary_type"],
      # explicitly *exclude* the grouping cols from the slice passed to collect_by_grade:
    )
    .apply(collect_by_grade, include_groups=False)
    .reset_index()
)

print(crime_with_grades.head())
print(len(crime_with_grades))
# 5) Save the joined DataFrame to a new parquet file if needed
crime_with_grades.to_parquet("Data/processed/crime_with_school_match.parquet", index=False)