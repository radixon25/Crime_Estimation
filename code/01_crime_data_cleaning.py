"""
crime_data_cleaning.py

Streams and processes the Chicago Crime dataset from Socrata, saving it in Parquet chunks for efficient downstream analysis.

Inputs:
    Chicago Crime API (Socrata, resource_id="ijzp-q8t2")

Outputs:
    Data/processed/crime_parquet/chunk_*.parquet : Parquet files with selected columns and cleaned types

Assumptions:
    - Environment variables for Socrata credentials are set in a .env file.
    - Only selected columns are kept for downstream use.
"""

# Initialize Python Packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sodapy import Socrata
from dotenv import load_dotenv
import os
import fastparquet
import pyarrow
import dask.dataframe as dd


# ── Load environment variables ──────────────────────────────────────────────
load_dotenv()
MyAppToken = os.getenv("MY_APP_TOKEN")
username = os.getenv("CHICAGO_USERNAME")
password = os.getenv("CHICAGO_PASSWORD")


# ── Set up authenticated Socrata client ────────────────────────────────────
client = Socrata(
    "data.cityofchicago.org",
    app_token=MyAppToken,
    username=username,
    password=password,
    timeout=240
)

# ── Stream and process the Chicago Crime dataset ────────────────────────────
resource_id = "ijzp-q8t2"
batch_size = 100_000
offset = 0
chunk_index = 0

# Make sure the output folder exists
os.makedirs("Data/processed/crime_parquet", exist_ok=True)

while True:
    # 4a) Fetch the next batch of up to `batch_size` rows
    results = client.get(resource_id, limit=batch_size, offset=offset)
    if not results:
        print(f"No more rows after offset {offset}. Done.")
        break

    # 4b) Convert this chunk of dicts directly into a DataFrame
    df_chunk = pd.DataFrame.from_records(results)
    print(f"Chunk {chunk_index}: fetched {len(df_chunk)} rows (offset {offset}).")

    # 4c) get variable types for documentation
    print(f"Chunk {chunk_index} variable types:")
    print(df_chunk.dtypes)

    # Keep only columns needed downstream
    desired_cols = [
        "id", "case_number", "date", "location_description",
        "arrest", "primary_type", "description", "fbi_code", "iucr",
        "beat", "ward", "year", "latitude", "longitude"
    ]
    keep_cols = [c for c in desired_cols if c in df_chunk.columns]
    df_chunk = df_chunk[keep_cols]

    # Convert string booleans to pandas BooleanDtype
    if "arrest" in df_chunk.columns:
        df_chunk["arrest"] = df_chunk["arrest"].map({"true": True, "false": False}).astype("boolean")
    if "domestic" in df_chunk.columns:
        df_chunk["domestic"] = df_chunk["domestic"].map({"true": True, "false": False}).astype("boolean")

    # Cast low-cardinality string fields to 'category'
    for cat_col in ["primary_type", "fbi_code", "location_description"]:
        if cat_col in df_chunk.columns:
            df_chunk[cat_col] = df_chunk[cat_col].astype("category")

    # Parse the date column
    if "date" in df_chunk.columns:
        df_chunk["date"] = pd.to_datetime(df_chunk["date"], errors="coerce", utc=True)

    # Downcast numeric columns
    for int_col in ["ward", "community_area", "year", "beat"]:
        if int_col in df_chunk.columns:
            df_chunk[int_col] = pd.to_numeric(df_chunk[int_col], errors="coerce").astype("Int32")
    if "latitude" in df_chunk.columns:
        df_chunk["latitude"] = pd.to_numeric(df_chunk["latitude"], errors="coerce").astype("float32")
    if "longitude" in df_chunk.columns:
        df_chunk["longitude"] = pd.to_numeric(df_chunk["longitude"], errors="coerce").astype("float32")
    # Print the first few rows of the cleaned chunk

    # 4d) Write each processed chunk directly to Parquet on disk
    out_path = f"Data/processed/crime_parquet/chunk_{chunk_index:04d}.parquet"
    df_chunk.to_parquet(out_path, index=False)

    # 4e) Advance the offset & chunk index, then loop
    offset += batch_size
    chunk_index += 1

    # If this final chunk was smaller than batch_size, we know we’re done
    if len(df_chunk) < batch_size:
        print(f"Final chunk {chunk_index-1} had {len(df_chunk)} rows. Streaming complete.")
        break

# Print the cleaned columnd types for verification
print(f"Chunk {chunk_index} cleaned column types:")
print(df_chunk.dtypes)



print("Finished streaming all crime data chunks to Parquet.")


