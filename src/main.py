# main.py
import config,fnmatch,os
import pandas as pd
import logging
from datetime import datetime
from etl.etl import CSVLoader, JSONLoader, TextLoader
from database.config import get_config, get_db_engine
from database.database_writer import DatabaseWriter

logger = logging.getLogger(__name__)
logger.info("Application started.")

# Access directories
data_directory = config.get('Paths','data_directory')
log_directory = config.get('Paths','log_directory')
processed_directory = config.get('Paths', 'processed_directory')

# Configure logging to write to a file
log_path = os.path.join(log_directory, "etl.log")

logging.basicConfig(
    level= logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers= [
        logging.FileHandler(log_path, encoding='utf-8', mode='a'),
        logging.StreamHandler()
    ]
)

# Get ConfigParser object
config = get_config()
if not config:
    logger.error("Failed to load configuration. Exiting.")
    exit(1)
# ===============================================
    

merged_df = pd.DataFrame()
df_master = pd.DataFrame()

csv_frames = []
json_frames = []
text_frames = []

engine = None

try:
    engine = get_db_engine()
    if not engine:
        logger.error("Failed to create engine.")
        exit(1)
        
    writer = DatabaseWriter(engine)

    for filename in os.listdir(data_directory):
        fullpath = os.path.join(data_directory, filename) 
        try:
            if fnmatch.fnmatch(filename, '*.csv'):
                loader = CSVLoader(fullpath)
            elif fnmatch.fnmatch(filename, '*.json'):
                loader = JSONLoader(fullpath)
            elif fnmatch.fnmatch(filename, '*.txt'):
                loader = TextLoader(fullpath)
            else:
                logger.warning(f"Unsupported file format: {filename}")
                continue

            df = loader.load()
            if df is not None and not df.empty:
                if isinstance(loader, CSVLoader):
                    csv_frames.append(df)
                    logger.info(f"Preparing to insert {len(df)} rows")
                    writer.load_to_database(df, 'ads_data')
                elif isinstance(loader, JSONLoader):
                    json_frames.append(df)
                    logger.info(f"Preparing to insert {len(df)} rows")
                    writer.load_to_database(df, 'performance_data')
                elif isinstance(loader, TextLoader):
                    text_frames.append(df)
                    logger.info(f"Preparing to insert {len(df)} rows")
                    writer.load_to_database(df, 'clickstreams_data')
            else:
                logger.warning(f"No data returned for {filename}")
        except Exception as error:
            logger.error(f"Failed to load {filename}:{error}")
except Exception as error:
    logger.error(f"Error reading files into dataframe and database: {error}")  
    
# Merge all files together        
csv_df = pd.concat(csv_frames, ignore_index=True) if csv_frames else pd.DataFrame()
json_df = pd.concat(json_frames, ignore_index=True) if json_frames else pd.DataFrame()
text_df = pd.concat(text_frames, ignore_index=True) if text_frames else pd.DataFrame()

# Join
if not csv_df.empty and not json_df.empty:
    merged_df = csv_df.merge(json_df, on=['client', 'date', 'channel'], how= 'left')
else:
    merged_df = csv_df
    
if not merged_df.empty and not text_df.empty:   
    df_master = merged_df.merge(text_df, on=['client', 'date', 'channel'], how= 'left') 
else:
    df_master = merged_df

logger.info(f"File ingestion finished...")

DatabaseWriter.report_table(engine)

# Save the master report
logger.info(f"Write CSV report to {processed_directory}")
path = os.path.join(processed_directory, f"summary_report_{datetime.now().strftime("%Y%m%d")}.csv")
#df_master = DatabaseWriter.build_metadata("merged_pipeline", data_directory, df_master)
df_master.to_csv(path, index=False)