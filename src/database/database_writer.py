import os
import logging
import pandas as pd
import datetime

logger = logging.getLogger(__name__)


class DatabaseWriter:
    def __init__(self, engine):
        self.engine = engine

    def load_to_database(self, df: pd.DataFrame, table_name: str):
        logger.info(f"Attempting to load data for table: {table_name}")

        try:
            if self.engine is not None:
                df.to_sql(
                    name=table_name,
                    con=self.engine,
                    if_exists="replace",
                    index=False
                )

                logger.info(f"Sucessfully wrote:"
                            f"{len(df)} rows to {table_name}")
            else:
                logger.info(f"Error writing dataframe to {table_name}: {error}")
        except Exception as error:
            print(f"Unexpected error occurred: {error}")

    def build_metadata(file, data_directory, df):
        metadata_row = {
            "file_name": file,
            "file_type": os.path.splitext(file)[-1].replace(".", ""),
            "ingestion_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "row_count": len(df),
            "columns": ",".join(df.columns),
            "source_path": os.path.join(data_directory, file),
        }

        df = pd.concat([df, pd.DataFrame([metadata_row])], ignore_index=True)

        return df

    def report_table(engine):
        print("\n --- Reported Metric Data ---")

        query1 = (
            "SELECT channel, SUM(spend_usd) FROM"
            "ads_data GROUP BY channel"
            )
        result1 = pd.read_sql(query1, engine)
        print("\n--- Spend by Channel ---")
        print(result1.to_string())

        query2 = (
            "SELECT channel, SUM(clicks), SUM(conversions) FROM"  
            "performance_data GROUP BY channel"
            )
        result2 = pd.read_sql(query2, engine)
        print("\n--- Total clicks and conversions by Channel ---")
        print(result2.to_string())
