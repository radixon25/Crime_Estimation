import streamlit as st
import geopandas as gpd
import pandas as pd

@st.cache_data
def load_schools():
    # load your GeoParquet of school boundaries & attributes
    gdf = gpd.read_parquet("Data/processed/school_shapes.parquet")
    # compute centroid lat/lon for mapping
    gdf["latitude"] = gdf.geometry.centroid.y
    gdf["longitude"] = gdf.geometry.centroid.x
    return gdf

st.title("Chicago Schools Lookup")

schools = load_schools()

# sidebar inputs
query = st.sidebar.text_input("Search by School ID or Name", "")
grade_filter = st.sidebar.multiselect(
    "Grade Category", options=schools.GRADE_CAT.unique().tolist(), default=[]
)

# filter by grade if selected
df = schools.copy()
if grade_filter:
    df = df[df.GRADE_CAT.isin(grade_filter)]

# filter by text query
if query:
    if query.isdigit():
        df = df[df.SCHOOL_ID.astype(str).str.contains(query)]
    else:
        df = df[df.SCHOOL_NM.str.contains(query, case=False, na=False)]

st.write(f"## {len(df):,} schools matched")
st.dataframe(
    df[["SCHOOL_ID","SCHOOL_NM","GRADE_CAT","SCHOOL_ADD","file_year","latitude","longitude"]].sort_values("SCHOOL_NM"),
    height=300
)

if not df.empty:
    st.write("### Map view of results")
    st.map(df[["latitude","longitude"]])