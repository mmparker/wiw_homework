

import polars as pl
import os

baseurl = "https://public.wiwdata.com/engineering-challenge/data/"

# Build list of files
# Didn't seem to have permissions to list the files, but I could download - so I
# just built the file list manually.
# Create a dummy function here that would list the files, but just returns this instead:
filepaths = [ baseurl + letter + ".csv" for letter in "abcdefghijklmnopqrstuvwxyz" ]

# Read the files into a dict
# Error handling for: no file found, wrong file type
df_dict = { os.path.basename(path): pl.read_csv(path) for path in filepaths }


# Validate rows, columns, types
# Minimum rows, set columns, set types, identical columns, identical types
# Check for missing values
row_counts = { filename: df.shape[0] for filename, df in df_dict.items() }
column_names = { filename: df.columns for filename, df in df_dict.items() }
column_types = { filename: df.dtypes for filename, df in df_dict.items() }

row_counts
column_names
column_types

# Combine
df_list = [df for filename, df in df_dict.items()]
full_df = pl.concat(df_list)
full_df


# Aggregate
results = (
    full_df
        .group_by("user_id", "path")
        .agg(
            pl.len().alias("n_records"),
            pl.sum("length").alias("total_seconds")
        )
)

results.pivot(values = "total_seconds", index = "user_id", columns = "path", aggregate_function = "sum")


# Write to CSV