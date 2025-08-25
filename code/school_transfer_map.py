import geopandas as gpd
import pandas as pd
import folium
from shapely.geometry import mapping

# Load data
closure_df = pd.read_csv("Data/processed/school_closure_years.csv")
area_transfers_df = pd.read_csv("Data/processed/school_area_transfers.csv")

# 5) now loop & draw, but use schools_map_gdf so all geometries are lat/lon:
for year in range(2008, 2019):
    prev_map = schools_map_gdf[schools_map_gdf.academic_year_start == year]
    next_map = schools_map_gdf[schools_map_gdf.academic_year_start == year + 1]
    transfers = area_transfers_df[area_transfers_df.closure_year == year]

    m = folium.Map(location=[41.8781, -87.6298], zoom_start=11,
                   tiles="CartoDB positron")

    # 1) create one FeatureGroup per grade & year
    for grade, col in [("ES","blue"), ("MS","green"), ("HS","purple")]:
        fg_prev = folium.FeatureGroup(name=f"{grade} {year}", show=False)
        fg_next = folium.FeatureGroup(name=f"{grade} {year+1}", show=False)

        for _, r in prev_map[prev_map.GRADE_CAT == grade].iterrows():
            folium.GeoJson(
                mapping(r.geometry),
                style_function=lambda feat, c=col: {
                    "color": c, "fillColor": c, "fillOpacity":0.15, "weight":2
                },
                tooltip=f"{r.SCHOOL_NM} ({grade}) – {year}"
            ).add_to(fg_prev)

        for _, r in next_map[next_map.GRADE_CAT == grade].iterrows():
            folium.GeoJson(
                mapping(r.geometry),
                style_function=lambda feat, c=col: {
                    "color": c, "fillColor": c, "fillOpacity":0.05, "weight":2
                },
                tooltip=f"{r.SCHOOL_NM} ({grade}) – {year+1}"
            ).add_to(fg_next)

        fg_prev.add_to(m)
        fg_next.add_to(m)

    # 2) a single “Transferred Areas” layer
    fg_transfer = folium.FeatureGroup(name="Transferred Areas", show=True)
    for _, t in transfers.iterrows():
        closed = prev_map.loc[prev_map.SCHOOL_ID == t.Closed_SCHOOL_ID, "geometry"].iloc[0]
        recv   = next_map.loc[next_map.SCHOOL_ID == t.Receiving_SCHOOL_ID, "geometry"].iloc[0]
        patch  = closed.intersection(recv)
        if not patch.is_empty:
            folium.GeoJson(
                mapping(patch),
                style_function=lambda feat: {
                    "color":"orange","fillColor":"yellow",
                    "fillOpacity":0.5,"weight":2
                },
                tooltip=(
                    f"from {t.Closed_SCHOOL_NM} → {t.Receiving_SCHOOL_NM}<br>"
                    f"{t.transferred_area_sqm:.0f} m²"
                )
            ).add_to(fg_transfer)
    fg_transfer.add_to(m)

    # 3) add layer control
    folium.LayerControl(collapsed=False).add_to(m)

    out = f"Data/processed/school_boundary_transfers_{year}_{year+1}.html"
    m.save(out)
    print("saved", out)
