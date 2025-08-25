import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# 1) Load crime incidents at schools (or all crime points)
crime_df = pd.read_parquet("Data/processed/crime_at_schools.parquet")
# Ensure latitude/longitude exist
crime_df = crime_df.dropna(subset=["latitude", "longitude"])

# 2) Convert crimes to a GeoDataFrame
crime_gdf = gpd.GeoDataFrame(
    crime_df,
    geometry=gpd.points_from_xy(crime_df.longitude, crime_df.latitude),
    crs="EPSG:4326"
)

# 3) Load school boundaries and get centroids
schools_gdf = gpd.read_parquet("Data/processed/school_shapes.parquet")
schools_gdf = schools_gdf.set_crs("EPSG:4326", allow_override=True)
# Compute centroids for nearest‐neighbor matching
schools_centroids = schools_gdf.copy()
schools_centroids["geometry"] = schools_centroids.geometry.centroid

# ── 3.5) Ensure both GeoDataFrames have a proper CRS before projecting ──
# (if not already set)
if crime_gdf.crs is None or crime_gdf.crs.to_string().startswith("EPSG:4326") is False:
    crime_gdf = crime_gdf.set_crs("EPSG:4326", allow_override=True)
if schools_centroids.crs is None or schools_centroids.crs.to_string().startswith("EPSG:4326") is False:
    schools_centroids = schools_centroids.set_crs("EPSG:4326", allow_override=True)

# ── 4) Reproject both to a metric CRS (EPSG:3857) and spatial-join nearest ──
crime_3857             = crime_gdf.to_crs(epsg=3857)
schools_centroids_3857 = schools_centroids.to_crs(epsg=3857)

assigned = gpd.sjoin_nearest(
    crime_3857,
    schools_centroids_3857[["SCHOOL_ID", "SCHOOL_NM", "GRADE_CAT", "geometry"]],
    how="left",
    distance_col="distance_m"  # now correctly in meters
)

# ── 5) (Optional) Reproject back to WGS84 for lat/lon if you need it downstream ──
assigned = assigned.to_crs(epsg=4326)

# 6) Save the result
#    Contains crime fields + nearest SCHOOL_ID, SCHOOL_NM, GRADE_CAT, distance_m
assigned.drop(columns="geometry").to_parquet(
    "Data/processed/crime_with_nearest_school.parquet",
    index=False
)
print(f"Assigned {len(assigned)} crimes to nearest schools.")

# Load both datasets for comparison
nearest_df = pd.read_parquet("Data/processed/crime_with_nearest_school.parquet")
match_df   = pd.read_parquet("Data/processed/crime_with_school_match.parquet")


# 8) For each crime, check which years its nearest‐school polygon contains it
def find_containing_years(crime_row):
    sid = crime_row["SCHOOL_ID"]
    pt = crime_row.geometry
    # filter to this school’s boundaries
    polys = schools_gdf[schools_gdf["SCHOOL_ID"] == sid]
    # find all years where the polygon contains the point
    yrs = polys[polys.geometry.contains(pt)]["file_year"].tolist()
    return yrs

# Apply to the assigned GeoDataFrame
assigned["years_in_boundary"] = assigned.apply(find_containing_years, axis=1)

# 9) Build a flat review DataFrame (use a list, not a set, for indexing)
cols = [
    "date", "id", "latitude", "longitude",
    "SCHOOL_ID", "SCHOOL_NM", "GRADE_CAT",
    "nearest_distance_m", "years_in_boundary"
]

review_df = (
    assigned
      .rename(columns={"distance_m": "nearest_distance_m"})
      [cols]
)

# 10) Save or inspect
review_df.to_parquet(
    "Data/processed/crime_boundary_containment_review.parquet",
    index=False
)
print("Containment review saved:", review_df.shape)
print(review_df.head())

# 11) Keep only crimes whose date‐year suffix matches one of the boundary years
#    e.g. date 2012-XX-XX → suffix '12' → match against ['1112','1213',…]
review_df["yr_suffix"] = review_df["date"].dt.strftime("%y")

mask = review_df.apply(
    lambda row: any(yr.endswith(row["yr_suffix"]) for yr in row["years_in_boundary"]),
    axis=1
)

filtered_df = review_df[mask].drop(columns="yr_suffix").copy()

# 12) Save or inspect the filtered results
filtered_df.to_parquet(
    "Data/processed/crime_boundary_containment_filtered.parquet",
    index=False
)
print(f"Filtered containment review: {len(filtered_df)} of {len(review_df)}")