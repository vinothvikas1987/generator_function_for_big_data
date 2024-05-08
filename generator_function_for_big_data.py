
import pandas as pd

df_par_chunk = pd.read_parquet(r"")

df_csv = pd.read_csv('')

import numpy as np
def convert_to_uint(df_par_chunk, column_name):
    # Find unique values in the column
    unique_values = df_par_chunk[column_name].unique()

    # Create a mapping of unique values to integer values
    value_to_int = {value: index+243 for index, value in enumerate(unique_values)}

    # Define a generator to map values to integers
    def value_generator():
        for value in df_par_chunk[column_name]:
            yield value_to_int[value]

    # Convert the data type to uint
    df_par_chunk[column_name] = np.fromiter(value_generator(), dtype='uint16')

# Call the function to convert the column to uint
convert_to_uint(df_par_chunk, 'series_id')

df_par_chunk.head(2)

df_par_chunk['timestamp'] = pd.to_datetime(df_par_chunk['timestamp'],utc=True)
df_par_chunk['timestamp'].dtype

df_par_chunk['hour_min_sec'] = df_par_chunk['timestamp'].dt.strftime('%H:%M:%S')
df_par_chunk['day_of_week'] = df_par_chunk['timestamp'].dt.day_name()

df_par_chunk = df_par_chunk.drop(['timestamp'], axis=1)

df_par_chunk.head(1)

def convert_days_to_int(df_par_chunk):
    day_to_int = {
        'Monday': 1,
        'Tuesday': 2,
        'Wednesday': 3,
        'Thursday': 4,
        'Friday': 5,
        'Saturday': 6,
        'Sunday': 7
    }
    for index, row in df_par_chunk.iterrows():
        day = row['day_of_week']
        if day in day_to_int:
            yield day_to_int[day]
        else:
            yield None  # Handle invalid days

# Use the generator to create a new column 'days_int'
df_par_chunk['day_of_week'] = list(convert_days_to_int(df_par_chunk))

# Convert the 'days_int' column to uint datatype
df_par_chunk['day_of_week'] = df_par_chunk['day_of_week'].astype('uint8')

df_par_chunk['series_id'].unique()

def convert_to_uint(df_csv, column_name):
    # Find unique values in the column
    unique_values = df_csv[column_name].unique()

    # Create a mapping of unique values to integer values
    value_to_int = {value: index for index, value in enumerate(unique_values)}

    # Define a generator to map values to integers
    def value_generator():
        for value in df_csv[column_name]:
            yield value_to_int[value]

    # Convert the data type to uint
    df_csv[column_name] = np.fromiter(value_generator(), dtype='uint16')

# Call the function to convert the column to uint
convert_to_uint(df_csv, 'series_id')

df_csv['series_id'].unique()

df_csv = df_csv.dropna(subset=['step'])

df_par_chunk.dtypes

df_csv.shape

def fill_night_values(group):
    result = []
    counter = 1
    for _, row in group.iterrows():
        result.extend([counter] * len(group))
        counter += 1
    return pd.Series(result, dtype='uint8')

# Group by 'series_id' and apply the custom function
result_df = df_csv.groupby('series_id', group_keys=False).apply(fill_night_values)

df_csv['group_length'] = df_csv.groupby('series_id')['series_id'].transform('count')

# Create a new column 'night_' with ascending values in pairs
df_csv['night_'] = (df_csv.groupby('series_id').cumcount() // 2) + 1

df_par_chunk.head(2)

df_csv.head(2)

df_csv_= df_csv.drop(['night','group_length','timestamp'],axis=1)

df_csv_['step'] = df_csv_['step'].astype('int64')
df_csv_['series_id'] = df_csv_['series_id'].astype('uint16')
df_csv_['night_'] = df_csv_['night_'].astype('uint8')

df_csv_.dtypes

df_csv_['event'].isna().sum()

event_mapping = {'onset': 0, 'wakeup': 1}

df_csv_['event'] = df_csv_['event'].map(event_mapping)

df_par_chunk.head(4)

df_csv_.head(4)

df = pd.merge(df_par_chunk, df_csv_, on=['series_id', 'step'], how='left')

df.head(3)

filled_values = []
current_value = 1

for value in df['event']:
    if pd.notna(value):
        current_value = value
    if pd.notna(current_value):
        filled_values.append(current_value)
    elif current_value == 0:
        filled_values.append(0)
    else:
        filled_values.append(1)

# Fill in any remaining NaN values with 1
filled_values += [1] * (len(df) - len(filled_values))

df['event'] = filled_values

df['event'] = df['event'].astype('uint8')

df.head(3)

df.to_parquet('merged_10_night.parquet')

from IPython.display import FileLink
FileLink('merged_10_night.parquet')

final_df = df.drop('night_',axis=1)

final_df.to_parquet('merged_10.parquet')

from IPython.display import FileLink
FileLink('merged_10.parquet')















