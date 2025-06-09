#  - Example using Dask DataFrame:
import dask.dataframe as dd
import glob
import pandas as pd
import geopandas as gpd
import folium
from folium.plugins import MarkerCluster
from shapely.geometry import Point
import numpy as np
from folium.plugins import TimestampedGeoJson
import json
from shapely.geometry import mapping



# ────────────────────────────────────────────────────────────────
# 1) LOAD YOUR SCHOOLS AND CONVERT file_year → academic_year_start
# ────────────────────────────────────────────────────────────────

schools_gdf = gpd.read_parquet("Data/processed/school_shapes.parquet")
schools_gdf = schools_gdf.set_crs("EPSG:4326", allow_override=True)

# file_year like "1011" → int("10") + 2000 = 2010
schools_gdf["academic_year_start"] = (
    schools_gdf["file_year"].str[:2].astype(int) + 2000
)

# (Optional) also get the July 1 date if you want a datetime
schools_gdf["academic_year_date"] = pd.to_datetime(
    schools_gdf["academic_year_start"].astype(str) + "-07-01"
)

# ────────────────────────────────────────────────────────────────
# 2) LOAD YOUR CRIMES AND COMPUTE EACH POINT’S academic_year_start
# ────────────────────────────────────────────────────────────────

# Merge your match‐info with raw lat/lon
df_match = pd.read_parquet("Data/processed/crime_with_school_match.parquet")
df_points = pd.read_parquet("Data/processed/crime_at_schools.parquet")

crime_df = pd.merge(
    df_match,
    df_points[["id","latitude","longitude"]],
    on="id",
    how="left",
    validate="one_to_one"
)

# Drop any missing coords
crime_df = crime_df.dropna(subset=["latitude","longitude"])

# Parse date
crime_df["date"] = pd.to_datetime(crime_df["date"])

# academic_year_start = year if month >= 7 else year-1
crime_df["academic_year_start"] = np.where(
    crime_df["date"].dt.month >= 7,
    crime_df["date"].dt.year,
    crime_df["date"].dt.year - 1
)


# Build GeoDataFrame
crime_gdf = gpd.GeoDataFrame(
    crime_df,
    geometry=gpd.points_from_xy(crime_df.longitude, crime_df.latitude),
    crs="EPSG:4326"
)
# Loop over each school year
for year in range(2008, 2019):
    # 1) Filter to that academic year
    schools_year = schools_gdf[schools_gdf["academic_year_start"] == year]
    crime_year   = crime_gdf[crime_gdf["academic_year_start"] == year]
    
    # 2) Initialize the map
    m = folium.Map(location=[41.8781, -87.6298], zoom_start=11)
    
    # 3) Add school boundaries by grade category
    for grade, label, color in [
        ("ES","Elementary Schools","#1f78b4"),
        ("MS","Middle Schools",    "#33a02c"),
        ("HS","High Schools",      "#e31a1c"),
    ]:
        subset = schools_year[schools_year["GRADE_CAT"] == grade]
        if subset.empty:
            continue
        
        folium.GeoJson(
            {"type":"FeatureCollection",
             "features":[
               {
                 "type":"Feature",
                 "geometry": mapping(row.geometry),
                 "properties": {
                   "Name":        row.SCHOOL_NM,
                   "Level":       row.GRADE_CAT,
                   "Acad Year":   f"{row.academic_year_start}–{row.academic_year_start+1}"
                 }
               }
               for _, row in subset.iterrows()
             ]},
            style_function=lambda feat, col=color: {
                "fillColor":   col,
                "color":       col,
                "weight":      3,
                "fillOpacity": 0.2
            },
            name=label
        ).add_to(m)
    
    # 4) Add clustered crime points
    cluster = MarkerCluster(name="Crime Incidents").add_to(m)
    for _, r in crime_year.iterrows():
        popup_html = (
            f"<b>ID:</b> {r.id}<br>"
            f"<b>Date:</b> {r.date.date()}<br>"
            f"<b>Type:</b> {r.primary_type}"
        )
        folium.CircleMarker(
            location=[r.geometry.y, r.geometry.x],
            radius=3,
            color="black",
            fill=True,
            fill_color="yellow",
            fill_opacity=0.7,
            popup=popup_html,
            tooltip=popup_html
        ).add_to(cluster)
    
    # 5) Add layer control & save
    folium.LayerControl(collapsed=False).add_to(m)
    out_path = f"Data/processed/map_{year}_{year+1}.html"
    m.save(out_path)
    print(f"Saved map for {year}–{year+1} → {out_path}")
