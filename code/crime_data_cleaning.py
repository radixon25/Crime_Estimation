# Initialize Python Packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sodapy import Socrata
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Access them using os.getenv
MyAppToken = os.getenv("MY_APP_TOKEN")
username = os.getenv("CHICAGO_USERNAME")
password = os.getenv("CHICAGO_PASSWORD")



# Read in CSV file
df = pd.read_csv('Data/raw_data/raw_data_by_month_ward.csv')

# Display the first few rows of the DataFrame
print(df.head())

# Example authenticated client (needed for non-public datasets):
client = Socrata("data.cityofchicago.org", 
                 app_token=MyAppToken,
                 username=username,
                 password=password ,
                 timeout=240)



# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
batch_size = 100000
offset = 0
all_data = []

while True:
    results = client.get("ijzp-q8t2", limit=batch_size, offset=offset)
    if not results:
        break
    if len(results) < batch_size:
        print(f"Retrieved {len(results)}, for a total of {offset + len(results)} records")
        break
    all_data.extend(results)
    offset += batch_size
    print(f"Retrieved {offset} records")





# Convert to pandas DataFrame
crime_df = pd.DataFrame.from_records(all_data)
crime_df.to_csv('Data/raw_data/crime_data.csv', index=False)
print(len(crime_df))

arrests = client.get("dpt3-jri9", offset = 1000, limit = 700000)

arrests_df = pd.DataFrame.from_records(arrests)
print(len(arrests_df))

# Filter and display rows in arrests_df that have a matching case number in crime_df
matching_arrests = arrests_df[arrests_df['case_number'].isin(crime_df['case_number'])]
print("Matching arrests count:", len(matching_arrests))
print(matching_arrests.head())

matching_crime = crime_df[crime_df['case_number'].isin(arrests_df['case_number'])]
print("Matching crimes count:", len(matching_crime))
   