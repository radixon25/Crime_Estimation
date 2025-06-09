#  - Example using Dask DataFrame:
import dask.dataframe as dd
import glob
import pandas as pd

# 1) Point to the folder where your parquet chunks live
PARQUET_DIR = "Data/processed/crime_parquet/"

# 2) Use glob to list all chunk files
#    (adjust the pattern if your filenames are different)
all_paths = glob.glob(PARQUET_DIR + "chunk_*.parquet")

# 3) Prepare a list to collect filtered DataFrames
filtered_chunks = []

# 4) Loop over each parquet file, read only the columns you need, then filter
for path in all_paths:
    # (a) If you only need 'location_description' and maybe a few other columns,
    #     specify them here to avoid loading everything:
    usecols = ["id", "date", "primary_type", "location_description", "ward", "latitude", "longitude"]

    # (b) Read the chunk into a DataFrame
    available_cols = pd.read_parquet(path, engine='pyarrow').columns
    df = pd.read_parquet(path, columns=[c for c in usecols if c in available_cols])

    # (c) Filter rows where 'location_description' contains "School"
    #     Use case-sensitive or case-insensitive as you prefer:
    mask = df["location_description"].str.contains("School", case=False, na=False)
    df_school = df.loc[mask]

    # (d) If there are any rows left after filtering, keep them
    if not df_school.empty:
        filtered_chunks.append(df_school)

# 5) Concatenate all filtered chunks into one DataFrame (if it fits in memory)
if filtered_chunks:
    crime_at_schools = pd.concat(filtered_chunks, ignore_index=True)
else:
    crime_at_schools = pd.DataFrame(columns=usecols)

print(f"Total rows with “School” in location_description: {len(crime_at_schools)}")
print(crime_at_schools.head())

# 6) Explore the locations and types of crimes
print(crime_at_schools["primary_type"].value_counts())
unique_locations = crime_at_schools["location_description"].unique()
print(f"Unique locations with crimes at schools: {len(unique_locations)}")

# 7) Save the filtered DataFrame to a new parquet file if needed
crime_at_schools.to_parquet("Data/processed/crime_at_schools.parquet", index=False)
