"""
crime_boundary_map.py

Visualizes school boundaries and crime incidents on an interactive map, with time sliders by academic year.

Inputs:
    Data/processed/school_shapes.parquet         : School boundaries and attributes (GeoParquet)
    Data/processed/crime_with_school_match.parquet : Crime incidents matched to schools
    Data/processed/crime_at_schools.parquet      : Crime incidents at school locations

Outputs:
    Data/processed/schools_and_crime_time_slider.html : Interactive HTML map with time slider

Assumptions:
    - 'file_year' is a string like "1011" for the 2010–2011 school year.
    - All input files exist and have the expected columns.
"""

import dask.dataframe as dd
import glob
import pandas as pd
import geopandas as gpd
import folium
from folium.plugins import MarkerCluster, TimestampedGeoJson
from shapely.geometry import Point, mapping
import numpy as np
import json

# ────────────────────────────────────────────────────────────────
# 1) Load school boundaries and convert file_year → academic_year_start
# ────────────────────────────────────────────────────────────────

schools_gdf = gpd.read_parquet("Data/processed/school_shapes.parquet")
schools_gdf = schools_gdf.set_crs("EPSG:4326", allow_override=True)
schools_gdf["academic_year_start"] = (
    schools_gdf["file_year"].str[:2].astype(int) + 2000
)
schools_gdf["academic_year_date"] = pd.to_datetime(
    schools_gdf["academic_year_start"].astype(str) + "-07-01"
)

# ────────────────────────────────────────────────────────────────
# 2) Load crime data and compute academic_year_start for each incident
# ────────────────────────────────────────────────────────────────

df_match = pd.read_parquet("Data/processed/crime_with_school_match.parquet")
df_points = pd.read_parquet("Data/processed/crime_at_schools.parquet")

crime_df = pd.merge(
    df_match,
    df_points[["id", "latitude", "longitude"]],
    on="id",
    how="left",
    validate="one_to_one"
)
crime_df = crime_df.dropna(subset=["latitude", "longitude"])
crime_df["date"] = pd.to_datetime(crime_df["date"])
crime_df["academic_year_start"] = np.where(
    crime_df["date"].dt.month >= 7,
    crime_df["date"].dt.year,
    crime_df["date"].dt.year - 1
)
crime_gdf = gpd.GeoDataFrame(
    crime_df,
    geometry=gpd.points_from_xy(crime_df.longitude, crime_df.latitude),
    crs="EPSG:4326"
)

# ────────────────────────────────────────────────────────────────
# 3) Filter to a single academic year (example: 2010–2011)
# ────────────────────────────────────────────────────────────────

year = 2010
schools_year = schools_gdf[schools_gdf["academic_year_start"] == year]
crime_year = crime_gdf[crime_gdf["academic_year_start"] == year]

# ────────────────────────────────────────────────────────────────
# 4) Build the folium map with a time slider for schools and crimes
# ────────────────────────────────────────────────────────────────

m = folium.Map(location=[41.8781, -87.6298], zoom_start=11)

# Add school boundaries as time-enabled GeoJSON features
school_features = []
for _, row in schools_gdf.iterrows():
    school_features.append({
        "type": "Feature",
        "geometry": mapping(row.geometry),
        "properties": {
            "times": [row.academic_year_date.strftime("%Y-%m-%dT%H:%M:%SZ")],
            "style": {
                "color": {"ES": "#1f78b4", "MS": "#33a02c", "HS": "#e31a1c"}[row.GRADE_CAT],
                "weight": 4,
                "fillColor": {"ES": "#1f78b4", "MS": "#33a02c", "HS": "#e31a1c"}[row.GRADE_CAT],
                "fillOpacity": 0.2,
            },
            "popup": (
                f"<b>{row.SCHOOL_NM}</b><br>"
                f"Level: {row.GRADE_CAT}<br>"
                f"Year: {row.academic_year_start}–{row.academic_year_start+1}"
            )
        }
    })

TimestampedGeoJson(
    {"type": "FeatureCollection", "features": school_features},
    period="P1Y",
    add_last_point=False,
    auto_play=False,
    loop_button=True,
    date_options="YYYY",
    time_slider_drag_update=True,
).add_to(m)

# Add crime points as time-enabled GeoJSON features
crime_features = []
for _, row in crime_gdf.iterrows():
    crime_features.append({
        "type": "Feature",
        "geometry": mapping(row.geometry),
        "properties": {
            "times": [row.date.strftime("%Y-%m-%dT%H:%M:%SZ")],
            "style": {"color": "black", "radius": 4, "fillColor": "yellow", "fillOpacity": 0.7},
            "popup": (
                f"<b>ID:</b> {row.id}<br>"
                f"<b>Date:</b> {pd.to_datetime(row.date).date()}<br>"
                f"<b>Type:</b> {row.primary_type}<br>"
                f"Acad Yr: {row.academic_year_start}–{row.academic_year_start+1}"
            )
        }
    })

TimestampedGeoJson(
    {"type": "FeatureCollection", "features": crime_features},
    period="P1Y",
    add_last_point=False,
    auto_play=False,
    loop_button=True,
    date_options="YYYY",
    time_slider_drag_update=True,
).add_to(m)

folium.LayerControl(collapsed=False).add_to(m)
m.save("Data/processed/schools_and_crime_time_slider.html")

print('end of code')
