import os
import logging
import pandas as pd
from datetime import datetime

LOGGER = logging.getLogger(__name__)


class DatabaseWriter:
    def __init__(self, engine):
        self.engine = engine

    def load_to_database(self, df: pd.DataFrame, table_name: str):
        LOGGER.info(f"Attempting to load data for table: {table_name}")

        try:
            if self.engine is not None:
                df.to_sql(
                    name=table_name,
                    con=self.engine,
                    if_exists="replace",
                    index=False
                )

                LOGGER.info(f"Sucessfully wrote: " f"{len(df)} rows to {table_name}")
            else:
                LOGGER.error(f"Error writing dataframe to {table_name}.")
        except Exception as error:
            print(f"Unexpected error occurred: {error}")

    def build_metadata(file, data_directory, df):
        metadata_row = {
            "file_name": file,
            "file_type": os.path.splitext(file)[-1].replace(".", ""),
            "ingestion_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "row_count": len(df),
            "columns": ",".join(df.columns),
            "source_path": data_directory,
        }

        return metadata_row
