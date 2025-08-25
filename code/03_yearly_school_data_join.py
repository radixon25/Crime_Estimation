"""
yearly_school_data_join.py

Joins yearly school boundary CSVs, standardizes columns, and saves as a GeoParquet file.  
Also spatially joins crime points to school boundaries.

Inputs:
    Data/temp/*Attendance*.csv, *Boundaries*.csv : Yearly school boundary CSVs
    Data/processed/crime_at_schools.parquet      : Crime incidents at school locations

Outputs:
    Data/processed/school_shapes.parquet         : School boundaries and attributes (GeoParquet)
    Data/processed/crime_with_school_match.parquet : Crime incidents matched to schools

Assumptions:
    - Input CSVs have variable column names, handled by rename_map.
    - "the_geom" column contains WKT polygons.
    - Crime data is already filtered to school locations.
"""

import os
import glob
import pandas as pd
import re
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point
import fastparquet

# ── 1) Find and read yearly school boundary CSVs ───────────────────────────
csv_folder = "Data/temp"
keywords = ["Attendance", "Boundaries"]
matching_files = []
for keyword in keywords:
    matched = glob.glob(os.path.join(csv_folder, f"*{keyword}*.csv"))
    matching_files.extend(matched)
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

file_cols = file_columns_df(matching_files)

# ── 2) Standardize columns and concatenate ────────────────────────────────
dfs = []
for filepath in matching_files:
    try:
        df = pd.read_csv(filepath)
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
        m = re.search(r"SY(\d{4})", basename)
        if m:
            year_code = m.group(1)
            df["file_year"] = year_code
        else:
            m2 = re.search(r"(\d{4})", basename)
            if m2:
                df["file_year"] = m2.group(1)
            else:
                df["file_year"] = None
        dfs.append(df)
    except Exception as e:
        print(f"Error processing {filepath}: {e}")

school_shapes = pd.concat(dfs, ignore_index=True, sort=False)[['SCHOOL_ID', 'SCHOOL_NM', 'the_geom', 'SCHOOL_ADD', 'GRADE_CAT', 'BOUNDARYGR', 'file_year']]
print("Merged shape:", school_shapes.shape)
print("Columns in merged_high:", school_shapes.columns.tolist())

# ── 3) Convert WKT to geometry and save as GeoParquet ─────────────────────
school_shapes["geometry"] = school_shapes["the_geom"].apply(wkt.loads)
schools_gdf = gpd.GeoDataFrame(
    school_shapes.drop(columns=["the_geom"]),
    geometry="geometry",
    crs="EPSG:4326"
)
schools_gdf.to_parquet("Data/processed/school_shapes.parquet", index=False)
print("Total schools:", len(schools_gdf))

# ── 4) Spatial join: match crime points to school boundaries ──────────────
school_crime_df = pd.read_parquet("Data/processed/crime_at_schools.parquet")
school_crime_gdf = gpd.GeoDataFrame(
    school_crime_df,
    geometry=gpd.points_from_xy(school_crime_df.longitude, school_crime_df.latitude),
    crs="EPSG:4326"
)
schools_proj = schools_gdf.to_crs(epsg=3857)
crime_proj = school_crime_gdf.to_crs(epsg=3857)
joined_many = gpd.sjoin(
    crime_proj,
    schools_proj,
    how="left",
    predicate="within"
).to_crs(epsg=4326)

def collect_by_grade(df):
    return pd.Series({
        "ES_schools": df.loc[df.GRADE_CAT == "ES", "SCHOOL_ID"].dropna().unique().tolist(),
        "MS_schools": df.loc[df.GRADE_CAT == "MS", "SCHOOL_ID"].dropna().unique().tolist(),
        "HS_schools": df.loc[df.GRADE_CAT == "HS", "SCHOOL_ID"].dropna().unique().tolist(),
    })

crime_with_grades = (
    joined_many
    .groupby(
      ["id", "date", "primary_type"],
    )
    .apply(collect_by_grade, include_groups=False)
    .reset_index()
)

print(crime_with_grades.head())
print(len(crime_with_grades))
crime_with_grades.to_parquet("Data/processed/crime_with_school_match.parquet", index=False)