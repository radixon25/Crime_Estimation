"""
crime_boundary_map_test.py

Generates yearly interactive maps of school boundaries and crime incidents for each academic year.

Inputs:
    Data/processed/school_shapes.parquet         : School boundaries and attributes (GeoParquet)
    Data/processed/crime_with_school_match.parquet : Crime incidents matched to schools
    Data/processed/crime_at_schools.parquet      : Crime incidents at school locations

Outputs:
    Data/processed/map_<year>_<year+1>.html      : Interactive HTML map for each academic year

Assumptions:
    - 'file_year' is a string like "1011" for the 2010–2011 school year.
    - All input files exist and have the expected columns.
"""

import dask.dataframe as dd
import glob
import pandas as pd
import geopandas as gpd
import folium
from folium.plugins import MarkerCluster, HeatMap
from shapely.geometry import Point, mapping
import numpy as np
from folium.plugins import TimestampedGeoJson
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
# 3) Loop over each academic year and generate a map
# ────────────────────────────────────────────────────────────────

for year in range(2008, 2019):
    schools_year = schools_gdf[schools_gdf["academic_year_start"] == year]
    crime_year = crime_gdf[crime_gdf["academic_year_start"] == year]
    
    m = folium.Map(location=[41.8781, -87.6298], zoom_start=11)
    
    # Add school boundaries by grade category
    for grade, label, color in [
        ("ES", "Elementary Schools", "#1f78b4"),
        ("MS", "Middle Schools", "#33a02c"),
        ("HS", "High Schools", "#e31a1c"),
    ]:
        subset = schools_year[schools_year["GRADE_CAT"] == grade]
        if subset.empty:
            continue
        
        folium.GeoJson(
            {"type": "FeatureCollection",
             "features": [
                 {
                     "type": "Feature",
                     "geometry": mapping(row.geometry),
                     "properties": {
                         "Name": row.SCHOOL_NM,
                         "Level": row.GRADE_CAT,
                         "Acad Year": f"{row.academic_year_start}–{row.academic_year_start+1}"
                     }
                 }
                 for _, row in subset.iterrows()
             ]},
            style_function=lambda feat, col=color: {
                "fillColor": col,
                "color": col,
                "weight": 3,
                "fillOpacity": 0.2
            },
            name=label
        ).add_to(m)
    
    # ── Replace clustered crime points with HeatMaps ────────────────
    # Remove or comment out the MarkerCluster section below:
    # cluster = MarkerCluster(name="Crime Incidents").add_to(m)
    # for _, r in crime_year.iterrows():
    #     …CircleMarker(…)…

    # Instead, build one FeatureGroup per crime type
    crime_types = crime_year["primary_type"].unique().tolist()
    for ctype in crime_types:
        fg = folium.FeatureGroup(name=f"Heat: {ctype}", show=False)
        pts = crime_year.loc[
            crime_year["primary_type"] == ctype,
            ["latitude", "longitude"]
        ].values.tolist()
        if pts:
            HeatMap(
                data=pts,
                radius=10,     # size of each “hot spot”
                blur=15,       # smoothing factor
                min_opacity=0.3
            ).add_to(fg)
        fg.add_to(m)

    # Optionally add an “All Crimes” heatmap layer (on by default)
    fg_all = folium.FeatureGroup(name="Heat: All Crimes", show=True)
    all_pts = crime_year[["latitude", "longitude"]].values.tolist()
    HeatMap(
        data=all_pts,
        radius=8,
        blur=12,
        min_opacity=0.2
    ).add_to(fg_all)
    fg_all.add_to(m)

    # ── re-add layer control ────────────────────────────────────────
    folium.LayerControl(collapsed=False).add_to(m)

    out_path = f"Data/processed/map_{year}_{year+1}.html"
    m.save(out_path)
    print(f"Saved map for {year}–{year+1} → {out_path}")
