# main.py
import fnmatch
import os
import pandas as pd
import logging
import json
from datetime import datetime
from etl.etl import CSVLoader, JSONLoader, TextLoader
from database.config import get_config, get_db_engine
from database.database_writer import DatabaseWriter
from pathlib import Path

# Load config object
config = get_config()
if not config:
    print("Failed to load configuration. Exiting.")
    exit(1)

# Access directories
DATA_DIRECTORY = config.get("Paths", "data_directory")
LOG_DIRECTORY = config.get("Paths", "log_directory")
PROCESSED_DIRECTORY = config.get("Paths", "processed_directory")

os.makedirs(DATA_DIRECTORY, exist_ok=True)
os.makedirs(PROCESSED_DIRECTORY, exist_ok=True)
os.makedirs("logs", exist_ok=True)
os.makedirs("metadata", exist_ok=True)

# Configure logging
LOG_PATH = os.path.join(LOG_DIRECTORY, "etl.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8", mode="a"),
        logging.StreamHandler(),
    ],
)

# ===============================================

LOGGER = logging.getLogger(__name__)
LOGGER.info("Application started.")

merged_df = pd.DataFrame()
df_master = pd.DataFrame()

csv_frames = []
json_frames = []
text_frames = []

engine = None

try:
    engine = get_db_engine()
    if not engine:
        LOGGER.error("Failed to create engine.")
        exit(1)

    writer = DatabaseWriter(engine)

    for filename in os.listdir(DATA_DIRECTORY):
        fullpath = os.path.join(DATA_DIRECTORY, filename)
        try:
            if fnmatch.fnmatch(filename, "*.csv"):
                loader = CSVLoader(fullpath)
            elif fnmatch.fnmatch(filename, "*.json"):
                loader = JSONLoader(fullpath)
            elif fnmatch.fnmatch(filename, "*.txt"):
                loader = TextLoader(fullpath)
            else:
                LOGGER.warning(f"Unsupported file format: {filename}")
                continue

            df = loader.load()
            if df is not None and not df.empty:
                if isinstance(loader, CSVLoader):
                    csv_frames.append(df)
                    LOGGER.info(f"Preparing to insert {len(df)} rows")
                    writer.load_to_database(df, "ADS_Data")
                elif isinstance(loader, JSONLoader):
                    json_frames.append(df)
                    LOGGER.info(f"Preparing to insert {len(df)} rows")
                    writer.load_to_database(df, "Performance_Data")
                elif isinstance(loader, TextLoader):
                    text_frames.append(df)
                    LOGGER.info(f"Preparing to insert {len(df)} rows")
                    writer.load_to_database(df, "Clickstreams_Data")
            else:
                LOGGER.warning(f"No data returned for {filename}")
        except Exception as error:
            LOGGER.error(f"Failed to load {filename}:{error}")
except Exception as error:
    LOGGER.error(f"Error reading files into dataframe and database: {error}")

# Merge files together
csv_df = pd.concat(csv_frames,
                   ignore_index=True) if csv_frames else pd.DataFrame()
json_df = pd.concat(json_frames,
                    ignore_index=True) if json_frames else pd.DataFrame()
text_df = pd.concat(text_frames,
                    ignore_index=True) if text_frames else pd.DataFrame()

if not csv_df.empty and not json_df.empty:
    merged_df = csv_df.merge(json_df, on=["Client",
                                          "Date",
                                          "Channel"],
                             how="left")
else:
    raise ValueError("Error merging CSV and JSON dataframes.")

if not merged_df.empty and not text_df.empty:
    df_master = merged_df.merge(text_df, on=["Client",
                                             "Date",
                                             "Channel"],
                                how="left")
else:
    raise ValueError("Error merging Text dataframe into the final dataframe.")
LOGGER.info("File ingestion finished...")

# Save the master report
LOGGER.info(f"Write CSV report to {PROCESSED_DIRECTORY}")
path = os.path.join(PROCESSED_DIRECTORY,
                    f"summary_report_{datetime.now()}.csv")
df_master.to_csv(path, index=False)

# Save metadata report
df_metadata = DatabaseWriter.build_metadata(
    f"Metadata_{datetime.now()}", DATA_DIRECTORY, df_master
)

meta_f = f"Metadata_{datetime.now().strftime('%Y-%m-%d')}.json"
with open(f"Metadata/{meta_f}", "w") as file:
    json.dump(df_metadata, file, indent=4)
