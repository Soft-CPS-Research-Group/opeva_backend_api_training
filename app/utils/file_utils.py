import os, json, yaml, base64
from app.config import settings
from app.utils import mongo_utils
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import OrderedDict
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

# Utility function to convert timestamp strings to datetime objects
def parse_timestamp(ts):
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc)

    if isinstance(ts, str):
        try:
            return datetime.fromisoformat(ts).astimezone(timezone.utc)
        except ValueError:
            try:
                return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                try:
                    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                except ValueError:
                    raise ValueError(f"Invalid timestamp string format: {ts}")

    raise TypeError(f"Unsupported timestamp type: {type(ts)}")

def create_dataset_dir(name: str, site_id: str, config: dict, period: int = 60, from_ts: str = None, until_ts: str = None):
    # Create the target dataset directory
    path = os.path.join(settings.DATASETS_DIR, name)
    os.makedirs(path, exist_ok=True)

    # Connect to the MongoDB database for the given site
    db = mongo_utils.get_db(site_id)
    collection_names = db.list_collection_names()

    # Fetch the structure from the special "schema" collection
    structure_doc = db["schema"].find_one().get("schema")
    # TODO: retornar este erro para o pedido
    if not structure_doc:
        raise ValueError(f"Missing 'schema' collection in site '{site_id}'")
    # Saves the buildings ids present in the schema for future data fetch

    building_ids = list(structure_doc.get("buildings").keys())

    # TODO: Meto so corres que aparecem no schema?

    # Find collections that start with 'building_' followed by each building_id and are in the schema
    building_collections = [c for c in collection_names if
                            any(c.startswith(f"building_{building_id}") for building_id in building_ids)]
    # TODO tratar aqui para caso building_collections esteja vazio
    price_collection = building_collections[0]

    # Parse timestamp range if provided
    from_dt = parse_timestamp(from_ts) if from_ts else None
    until_dt = parse_timestamp(until_ts) if until_ts else None

    # Validate and adjust the requested date range based on actual data availability in MongoDB
    date_ranges = list_dates_available_per_collection(site_id)

    # Keep only records that match the relevant collections
    relevant_ranges = [r for r in date_ranges if r["installation"] in building_collections]

    if not relevant_ranges:
        raise ValueError("No available data found in the relevant collections.")

    # Find the most recent start (latest of the oldest records)
    latest_start = max(parse_timestamp(r["oldest_record"]) for r in relevant_ranges)
    # Find the earliest end (earliest of the newest records)
    earliest_end = min(parse_timestamp(r["newest_record"]) for r in relevant_ranges)

    # Adjust the range if the requested timestamps exceed what's available
    if not from_dt or from_dt < latest_start:
        from_dt = latest_start
    if not until_dt or until_dt > earliest_end:
        until_dt = earliest_end

    # Ensure the adjusted date range is valid
    if from_dt >= until_dt:
        raise ValueError("Invalid time range: no data available for the given time period.")

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
        
        return data

    def building_format(data_aggregated, file_name):

        # If there is missing data, in this step the data is filled in
        data_missing_indices_filled = interpolate_missing_values(data_aggregated)

        data_missing_indices_filled = {timestamp: values for timestamp, values in data_missing_indices_filled.items() if
                                       from_dt <= pd.to_datetime(timestamp) <= until_dt}

        data_missing_indices_filled = OrderedDict(sorted(data_missing_indices_filled.items()))

        with open(os.path.join(path, f"{file_name}.csv"), "w") as f:
            # Write the CSV header
            f.write(",".join(settings.BUILDING_DATASET_CSV_HEADER) + "\n")

            for timestamp, values in data_missing_indices_filled.items():
                if not isinstance(timestamp, datetime):
                    timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

                ts_data = {
                    "month": timestamp.month,
                    "hour": timestamp.hour,
                    "minutes": timestamp.minute,
                    "day_type": timestamp.weekday(),
                    "daylight_savings_status": is_daylight_savings(timestamp)
                }

                # Construct a CSV row
                row = []
                for field in settings.BUILDING_DATASET_CSV_HEADER:
                    if field in ts_data:
                        # Replace "timestamp" value with its components
                        row.append(str(ts_data.get(field, "")))
                    else:
                        row.append(str(values.get(field, "")))

                # Write the row to the file
                f.write(",".join(map(str, row)) + "\n")

    def price_format(data_aggregated, file_name):

        # If there is missing data, in this step the data is filled in
        data_missing_indices_filled = interpolate_missing_values(data_aggregated)

        data_missing_indices_filled = {timestamp: values for timestamp, values in data_missing_indices_filled.items() if
                                       from_dt <= pd.to_datetime(timestamp) <= until_dt}

        data_missing_indices_filled = OrderedDict(sorted(data_missing_indices_filled.items()))

        with open(os.path.join(path, f"{file_name}.csv"), "w") as f:
            # Write the CSV header
            f.write(",".join(settings.PRICE_DATASET_CSV_HEADER) + "\n")

            for timestamp, values in data_missing_indices_filled.items():
                row = []

                row.append(values.get("electricity_pricing", 0))
                for j in range(1, 4):
                    next_key = timestamp + pd.Timedelta(minutes=j * period)
                    row.append(
                        data_missing_indices_filled
                        .get(next_key, {})
                        .get("electricity_pricing", 0)
                    )

                # Write the row to the file
                f.write(",".join(map(str, row)) + "\n")

    def ev_format(data_aggregated, filename):


        with open(os.path.join(path, f"{file_name}.csv"), "w") as f:
            # Write the CSV header
            f.write(",".join(settings.EV_DATASET_CSV_HEADER) + "\n")

            for timestamp, values in data_missing_indices_filled.items():
                row = []
                for field in settings.EV_DATASET_CSV_HEADER:
                    row.append(str(values.get(field, "")))

                # Write the row to the file
                f.write(",".join(map(str, row)) + "\n")


    # Function to export data from a collection into a CSV file
    def write_csv(docs, header, file_name):
        # This step aggregates the data over the period specified as a parameter.
        # The aggregation is performed based on the column labels, applying specific aggregation methods,
        # such as summing all values or calculating the average, as defined in the provided aggregation rules.
        data_aggregated = data_format(docs, header)
        print(data_aggregated)
        if header == settings.PRICE_DATASET_CSV_HEADER:
           price_format(data_aggregated, file_name)

        elif header == settings.BUILDING_DATASET_CSV_HEADER:
            building_format(data_aggregated, file_name)

        '''elif header == settings.EV_DATASET_CSV_HEADER:
            ev_format(data_aggregated, file_name)'''


    charging_sessions_by_charger = {}
    # Export all building-related collections
    for col in building_collections:
        collection = list(db[col].find())
        write_csv(collection, settings.BUILDING_DATASET_CSV_HEADER, col)
        for doc in collection:
            timestamp = doc["timestamp"]
            for session in doc.get("charging_sessions", []):
                charger_id = session.get("charger_id")
                if not charger_id:
                    continue

                state = 1

                # If there is not an user_id or power, it is considered that there is no car charging in the station
                if session.get("user_id") != "" and session.get("power") != 0:
                    state = 3

                session_data = {
                    "timestamp": timestamp,
                    "electric_vehicle_charger_state": state,
                    "power": session.get("power", 0),
                    "electric_vehicle_id": session.get("user_id", ""),
                    "electric_vehicle_battery_capacity_khw": 0,   # TODO arranjar o que meter aqui
                    "current_soc": session.get("soc", 0),
                    "electric_vehicle_departure_time": session.get("flexibility", {}).get("departure.time", 0),
                    "electric_vehicle_required_soc_departure": session.get("flexibility", {}).get("departure.soc", 0),
                    "electric_vehicle_estimated_arrival_time": session.get("flexibility", {}).get("arrival.time", 0),
                    "electric_vehicle_estimated_soc_arrival": 0   # TODO arranjar o que meter aqui
                }

                # TODO ir buscar ao schema o tamanho da bateria
                if charger_id not in charging_sessions_by_charger:
                    charging_sessions_by_charger[charger_id] = []

                charging_sessions_by_charger[charger_id].append(session_data)


    for charger in charging_sessions_by_charger.keys():
        write_csv(charging_sessions_by_charger.get(charger), settings.EV_DATASET_CSV_HEADER, charger)

    write_csv(list(db[price_collection].find()), settings.PRICE_DATASET_CSV_HEADER, "pricing")

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

def list_dates_available_per_collection(site_id: str):
    db = mongo_utils.get_db(site_id)

    # List all collections in the database
    collections = db.list_collection_names()

    results = []

    # Iterate over all collections in the database
    for collection_name in collections:
        if collection_name == "schema":
            continue

        collection = db[collection_name]

        # Find the oldest and newest documents based on 'timestamp'
        doc_oldest = collection.find_one(sort=[('_id', 1)])
        doc_newest = collection.find_one(sort=[('_id', -1)])

        # Parse and normalize timestamps
        ts_oldest = parse_timestamp(doc_oldest["timestamp"])
        ts_newest = parse_timestamp(doc_newest["timestamp"])

        # Append the results for this collection
        results.append({
            "installation": collection_name,
            "oldest_record": ts_oldest.isoformat(),
            "newest_record": ts_newest.isoformat()
        })

    return results

def list_available_datasets():
    return [d for d in os.listdir(settings.DATASETS_DIR) if os.path.isdir(os.path.join(settings.DATASETS_DIR, d))]

def delete_dataset_by_name(name: str) -> bool:
    path = os.path.join(settings.DATASETS_DIR, name)
    if os.path.exists(path) and os.path.isdir(path):
        shutil.rmtree(path)
        return True
    return False