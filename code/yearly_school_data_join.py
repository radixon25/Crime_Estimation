# Joining yearly reporting for CPS data

import os
import glob
import pandas as pd
import re


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

common_elem = file_columns_df(elementary_files)
print("Common columns in elementary files:")
print(common_elem)

common_middle = file_columns_df(middle_files)
print("Common columns in middle files:")
print(common_middle)

common_high = file_columns_df(high_files)
print("Common columns in high files:")


# Process high school files: standardize columns, add year column, then merge
dfs = []
for filepath in high_files:
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
            "SCHOOLID": "SCHOOL_ID"
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
merged_high = pd.concat(dfs, ignore_index=True, sort=False)
print("Merged shape:", merged_high.shape)
print("Columns in merged_high:", merged_high.columns.tolist())
