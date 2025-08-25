import pandas as pd
import os
from pathlib import Path

def get_file_info(filepath):
    """Get column names and types for a single file."""
    try:
        if filepath.suffix == '.csv':
            df = pd.read_csv(filepath, nrows=0)  # just headers
        elif filepath.suffix == '.parquet':
            df = pd.read_parquet(filepath)
            df = df.iloc[:0]  # keep structure, drop rows
        else:
            return None
        
        return {
            'file': filepath.name,
            'path': str(filepath),
            'columns': df.columns.tolist(),
            'dtypes': df.dtypes.to_dict(),
            'shape': f"({len(df.columns)} columns)"
        }
    except Exception as e:
        return {
            'file': filepath.name,
            'path': str(filepath),
            'error': str(e)
        }

def should_skip_file(filepath, seen_patterns):
    """Check if we should skip this file based on common naming patterns."""
    name = filepath.stem
    
    # Common patterns to group by
    patterns = [
        'crime_parquet_chunk_',
        'map_',
        'school_closures_2013_',
    ]
    
    for pattern in patterns:
        if name.startswith(pattern):
            if pattern in seen_patterns:
                return True
            seen_patterns.add(pattern)
            break
    
    return False

# Scan all CSV and Parquet files
project_root = Path(".")
files_to_scan = []
seen_patterns = set()

for ext in ['*.csv', '*.parquet']:
    for filepath in project_root.rglob(ext):
        if not should_skip_file(filepath, seen_patterns):
            files_to_scan.append(filepath)

# Get info for each file
file_infos = []
for filepath in sorted(files_to_scan):
    info = get_file_info(filepath)
    if info:
        file_infos.append(info)

# Create summary DataFrame
summary_data = []
for info in file_infos:
    if 'error' in info:
        summary_data.append({
            'File': info['file'],
            'Path': info['path'],
            'Columns': f"ERROR: {info['error']}",
            'Types': '',
            'Shape': ''
        })
    else:
        # Format column info nicely
        col_info = []
        for col in info['columns']:
            dtype = str(info['dtypes'][col])
            col_info.append(f"{col} ({dtype})")
        
        summary_data.append({
            'File': info['file'],
            'Path': info['path'],
            'Columns': ', '.join(info['columns']),
            'Types': ', '.join(col_info),
            'Shape': info['shape']
        })

summary_df = pd.DataFrame(summary_data)

# Save the overview
summary_df.to_csv("Data/processed/dataset_overview.csv", index=False)
print(f"Dataset overview saved with {len(summary_df)} files reviewed")
print("\nSample of files reviewed:")
print(summary_df[['File', 'Shape']].head(10))