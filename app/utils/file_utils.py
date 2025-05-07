import os, json, yaml, base64
from app.config import settings
from app.utils import mongo_utils
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
import shutil
import logging

def save_config_dict(config: dict, file_name: str) -> str:
    full_path = os.path.join(settings.CONFIGS_DIR, file_name)
    with open(full_path, "w") as f:
        yaml.dump(config, f)
    return file_name

def list_config_files():
    return [f for f in os.listdir(settings.CONFIGS_DIR) if f.endswith(('.yaml', '.yml'))]

def load_config_file(file_name):
    path = os.path.join(settings.CONFIGS_DIR, file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config {file_name} not found")
    with open(path) as f:
        return yaml.safe_load(f)

def delete_config_by_name(file_name):
    path = os.path.join(settings.CONFIGS_DIR, file_name)
    if os.path.exists(path):
        os.remove(path)
        return True
    return False

def collect_results(job_id):
    path = os.path.join(settings.JOBS_DIR, job_id, "results", "result.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"status": "pending", "message": "Result not ready yet."}

def read_progress(job_id):
    path = os.path.join(settings.JOBS_DIR, job_id, "progress", "progress.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"progress": "No updates yet."}

def create_dataset_dir(name: str, site_id: str, config: dict, period: int = 60, from_ts: str = None, until_ts: str = None):
    # Utility function to convert timestamp strings to datetime objects
    def parse_timestamp(ts: str) -> datetime:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

    # Create the target dataset directory
    path = os.path.join(settings.DATASETS_DIR, name)
    os.makedirs(path, exist_ok=True)

    # Connect to the MongoDB database for the given site
    db = mongo_utils.get_db(site_id)
    collection_names = db.list_collection_names()

    # Fetch the structure from the special "schema" collection
    structure_doc = db["schema"].find_one()
    if not structure_doc:
        raise ValueError(f"Missing 'schema' collection in site '{site_id}'")

    # Saves the buildings ids present in the schema for future data fetch
    building_ids = list(structure_doc.get("buildings").keys())

    # TODO: Add EV (electric vehicle) structure here

    # Find collections that start with 'building_' followed by each building_id
    # TODO: This logic will be updated later to use a specific prefix once Percepta side is updated
    building_collections = [c for c in collection_names if
                            any(c.startswith(building_id) for building_id in building_ids)]
    ev_collections = [c for c in collection_names if c.startswith("ev_")]
    price_collection = building_collections[0]

    # Parse timestamp range if provided
    from_dt = parse_timestamp(from_ts) if from_ts else None
    until_dt = parse_timestamp(until_ts) if until_ts else None

    # Utility function to determine if a datetime is in daylight savings time (Lisbon time zone)
    def is_daylight_savings(ts: datetime) -> int:
        ts_portugal = ts.astimezone(ZoneInfo("Europe/Lisbon"))
        return int(bool(ts_portugal.dst()))

    def data_format(mongo_docs, aggregation_rules):
        # Convert the raw MongoDB documents (list of dicts) into a DataFrame
        raw_data = pd.DataFrame(mongo_docs)

        # Ensure the 'timestamp' column is in datetime format with UTC timezone
        raw_data['timestamp'] = pd.to_datetime(raw_data['timestamp'], utc=True)

        # Set 'timestamp' as the index to allow time-based resampling
        raw_data.set_index('timestamp', inplace=True)

        filtered_rules = {
            col: rule
            for col, rule in aggregation_rules.items()
            if col in raw_data.columns and col not in settings.TIMESTAMP_DATASET_CSV_HEADER
        }

        # Resample and aggregate the data using the specified aggregation rules per column
        # Example of aggregation_rules: {'temperature': 'mean', 'load': 'sum'}
        aggregated_data = raw_data.resample(f'{period}min').agg(filtered_rules)

        # Create a full timestamp range to ensure completeness of the time series
        full_datetime_index = pd.date_range(start=from_dt, end=until_dt, freq=f'{period}min', tz='UTC')
        df_full = pd.DataFrame(full_datetime_index, columns=['timestamp'])

        # Merge the aggregated data with the full time range to fill any gaps
        df_complete = df_full.merge(aggregated_data, on='timestamp', how='outer')

        # Set 'timestamp' back as index and sort for chronological order
        df_complete.set_index('timestamp', inplace=True)
        df_complete.sort_index(inplace=True)

        # Return the fully aligned and aggregated DataFrame
        return df_complete

    def value(date, days, df, operation):
        n_days = 0
        x = 1
        there_is_no_data = False
        values = []
        while n_days != days and not there_is_no_data:
            if operation == 'sum':
                new_date = date + pd.Timedelta(days=x)
            else:
                new_date = date - pd.Timedelta(days=x)

            if new_date not in df.index:
                there_is_no_data = True
            elif not df.loc[new_date].isnull().all():
                values.append({'timestamp': new_date, **df.loc[new_date].to_dict()})
                n_days += 1
            x += 1

        return sorted(values, key=lambda item: item['timestamp'])

    def div_verification(s_date, f_date, days, df):
        div = days // 2
        rest = days % 2
        # values = Translator.value(sDate, div + rest, df, 'sub')
        values = value(s_date, days, df, 'sub')
        if len(values) >= div + rest:
            values2 = value(f_date, div, df, 'sum')
            if len(values2) == div:
                values = values[-(div + rest):]
                values.extend(values2)
            else:
                number_of_missing_values = div - len(values2)
                if number_of_missing_values < len(values) - div - rest:
                    values = values[-(div + rest - number_of_missing_values):]
                    values.extend(values2)
                else:
                    values.extend(values2)
        else:
            number_of_missing_values = div + rest - len(values)
            values2 = value(f_date, div + number_of_missing_values, df, 'sum')
            values.extend(values2)

        if len(values) < days:
            missing_values_count = days - len(values)
            for _ in range(missing_values_count):
                values.append({'Date': datetime(1, 1, 1, 0, 0), 'Value': 0})
        return values


    def interpolate_missing_values(df):
        data = {}  # Dictionary to store interpolated values for all columns
        indexs = []  # List to store indices of missing values
        x = 0  # Counter for consecutive missing values

        # Iterate over all rows in the DataFrame
        for i in range(len(df) + 1):
            if i != len(df):
                # For each row, add the values of all columns to the 'data' dictionary
                data[df.index[i]] = df.iloc[i].to_dict()

            # Check if any value in the row is NaN
            if i != len(df) and df.iloc[i].isnull().any():
                # If any value is missing, increment the counter and store the index
                x += 1
                indexs.append(df.index[i])
            elif x > 0:
                # If there were missing values, interpolate the values
                # TODO alterar o valor de 6 para um valor que é okay fazer interpolação considerando o periodo
                if x <= 6 and (indexs[0] - pd.Timedelta(hours=1)) in df.index and (
                        indexs[-1] + pd.Timedelta(hours=1)) in df.index:
                    # If the gap is small and there are valid values before and after
                    prev_values = df.loc[indexs[0] - pd.Timedelta(hours=1)].to_dict()  # Get the previous values
                    next_values = df.loc[indexs[-1] + pd.Timedelta(hours=1)].to_dict()  # Get the next values

                    # Perform linear interpolation for each column
                    # Considering the design of Percepta, if a column has NaN values, it indicates that Percepta did not record any data for that specific timestamp.
                    # # Therefore, when a value is NaN, it means no data was captured for that timestamp across all columns.
                    for col in df.columns:
                        values = np.linspace(prev_values[col], next_values[col], x + 2)

                        for j in range(x):
                            data[indexs[j]][col] = values[
                                j + 1]  # Assign interpolated values to the corresponding index

                else:
                    # If the gap is large, handle it by grouping by hour and applying specific logic
                    days_and_hours = {}
                    for j in range(x):
                        hour = indexs[j].hour
                        # Group missing indices by hour
                        if hour in days_and_hours:
                            days_and_hours[hour].append(indexs[j])
                            days_and_hours[hour] = sorted(days_and_hours[hour], key=lambda item: item.time())
                        else:
                            days_and_hours[hour] = [indexs[j]]

                    # Perform verification for each hour group
                    for hour in days_and_hours:
                        days = len(days_and_hours[hour])  # Number of missing entries in this hour
                        f_date = days_and_hours[hour][0]  # First date in the hour group
                        l_date = days_and_hours[hour][-1]  # Last date in the hour group
                        ver = div_verification(f_date, l_date, days, df)

                        # Assign verified values to the corresponding indices
                        for i in range(len(ver)):
                            data[days_and_hours[hour][i]] = ver[i]

                # Reset the counter and index list for the next batch of missing values
                x = 0
                indexs = []

        return [
            {'timestamp': ts, **row}
            for ts, row in sorted(data.items(), key=lambda x: x[0])
        ]

    def general_format(doc, is_timestamp_present, header):
        ts = doc.get("timestamp")
        ts_data = {}

        # Prepare timestamp-derived fields only if needed
        if is_timestamp_present and ts:

            if not isinstance(ts, datetime):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))

            ts_data = {
                "month": ts.month,
                "hour": ts.hour,
                "minutes": ts.minute,
                "day_type": ts.weekday(),
                "daylight_savings_status": is_daylight_savings(ts)
            }

        # Construct a CSV row
        row = []
        for field in header:
            if field in ts_data:
                # Replace "timestamp" value with its components
                row.append(str(ts_data.get(field, "")))
            else:
                row.append(str(doc.get(field, "")))

        return row

    # Function to export data from a collection into a CSV file
    def write_csv(docs, header, file_name):
        is_timestamp_present = False
        print(docs)
        # Check if timestamp-derived fields are needed
        if any(field in header for field in settings.TIMESTAMP_DATASET_CSV_HEADER):
            is_timestamp_present = True

        # This step aggregates the data over the period specified as a parameter.
        # The aggregation is performed based on the column labels, applying specific aggregation methods,
        # such as summing all values or calculating the average, as defined in the provided aggregation rules.
        data_aggregated = data_format(docs, header)

        # If there is missing data, in this step the data is filled in
        data_missing_indices_filled = interpolate_missing_values(data_aggregated)

        i = 0
        with open(os.path.join(path, f"{file_name}.csv"), "w") as f:
            # Write the CSV header
            f.write(",".join(header) + "\n")

            for doc in data_missing_indices_filled:
                row = []
                if header == settings.PRICE_DATASET_CSV_HEADER:
                    i+=1
                    row.append(doc.get("energy_price", 0))
                    row.append(docs[i + 1].get("energy_price", 0) if i + 1 < len(docs) else 0)
                    row.append(docs[i + 2].get("energy_price", 0) if i + 2 < len(docs) else 0)
                    row.append(docs[i + 3].get("energy_price", 0) if i + 3 < len(docs) else 0)

                else:
                    row = general_format(doc, is_timestamp_present, header)

                # Write the row to the file
                f.write(",".join(map(str, row)) + "\n")



    # Build MongoDB query for the time range
    query = {}
    if from_dt:
        query["timestamp"] = {"$gte": from_dt}
    if until_dt:
        if "timestamp" in query:
            query["timestamp"]["$lte"] = until_dt  + timedelta(minutes=period)
        else:
            query["timestamp"] = {"$lte": until_dt + timedelta(minutes=period)}

    # Export all building-related collections
    for col in building_collections:
        write_csv(list(db["R-H-01"].find(query)), settings.BUILDING_DATASET_CSV_HEADER, col)

    # Export all EV-related collections
    for col in ev_collections:
        write_csv(list(db[col].find(query)), settings.EV_DATASET_CSV_HEADER, col)

    if "timestamp" in query and "$lte" in query["timestamp"]:
        query["timestamp"]["$lte"] += timedelta(days=1)
    print(query)
    write_csv(list(db[price_collection].find(query)), settings.PRICE_DATASET_CSV_HEADER, "pricing")

    # Remove MongoDB _id if present
    structure_doc.pop("_id", None)

    # Combine the configuration with the structure and write to JSON
    schema = {
        **config,
        "structure": structure_doc
    }

    with open(os.path.join(path, "schema.json"), "w") as f:
        json.dump(schema, f, indent=2)

    return path

def list_available_datasets():
    return [d for d in os.listdir(settings.DATASETS_DIR) if os.path.isdir(os.path.join(settings.DATASETS_DIR, d))]

def delete_dataset_by_name(name: str) -> bool:
    path = os.path.join(settings.DATASETS_DIR, name)
    if os.path.exists(path) and os.path.isdir(path):
        shutil.rmtree(path)
        return True
    return False