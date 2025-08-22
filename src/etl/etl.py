#!/usr/bin/env python
# coding: utf-8
# etl.py

import pandas as pd
import os
import re
import json
import logging

logger = logging.getLogger(__name__)


class BaseLoader:

    def __init__(self, fullpath):
        """initializes instance variables of class"""
        self.fullpath = fullpath

    def _validate_file(self):
        """validates filenames and file structure"""
        if not os.path.exists(self.fullpath):
            logger.error("File doesn't exists. Check folders. ")
            raise FileNotFoundError(f"File in path {self.fullpath} not found.")

    def _read_source(self):
        """read in files"""
        with open(self.fullpath, "r") as f1:
            content = f1.read()
            return content

    def _parse_records(self, raw):
        """parse data records"""
        raise NotImplementedError("Subclasses must implement method.")

    def _to_dataframe(self, records):
        """converting data structures to dataframes"""
        if isinstance(records, pd.DataFrame):
            return records
        elif isinstance(records, list):
            if len(records) == 0:
                return pd.DataFrame()
            if all(isinstance(item, pd.DataFrame) for item in records):
                return pd.concat(records, ignore_index=True)
            elif all(isinstance(item, dict) for item in records):
                return pd.DataFrame(records)
            else:
                raise ValueError(
                    "List elements are neither all Dataframes"
                    "nor all dict-like objects."
                )
        else:
            raise TypeError("Unsupported type for records in _to_dataframe")

    def _postprocess_df(self, df):
        df = df.drop_duplicates()
        return df

    def load(self):
        """load data for database"""
        self._validate_file()
        raw = self._read_source()
        records = self._parse_records(raw)
        df = self._to_dataframe(records)
        df = self._postprocess_df(df)
        return df


class CSVLoader(BaseLoader):

    def __init__(self, fullpath, delimiter=","):
        super().__init__(fullpath)
        self.delimiter = delimiter
        self.filename = os.path.basename(self.fullpath)
        self.pattern = r"^AD_SPEND_[a-zA-Z0-9_]+_\d{8}.csv"

    def _validate_file(self):
        super()._validate_file()

        if not self.filename.endswith(".csv"):
            raise ValueError("Invalid file extension. Check file.")

        if re.match(self.pattern, self.filename, re.IGNORECASE) is None:
            logger.warning("Invalid CSV filename pattern. Check file.")

        req_columns = ["Date", "Channel", "Spend_usd", "Client"]

        try:
            header = pd.read_csv(self.fullpath, nrows=0)  # get header row only
            col_names = list(header.columns)

            if not all(col in col_names for col in req_columns):
                logger.warning(
                    f"CSV file {self.filename}"
                    f" is missing required columns. Skipping file.")
            else:
                logger.info(f"File {self.filename} passed validation check.")

        except Exception as error:
            logger.warning(f"File {self.filename}"
                           f"encountered a header error: {error}."
                           )

    def _read_source(self):
        return pd.read_csv(self.fullpath, sep=self.delimiter)

    def _parse_records(self, raw):
        return raw

    def _postprocess_df(self, df):
        df = super()._postprocess_df(df)
        df.columns = df.columns.str.lower()
        return df


class JSONLoader(BaseLoader):

    def __init__(self, fullpath):
        super().__init__(fullpath)
        self.filename = os.path.basename(self.fullpath)
        self.pattern = r"^PERFORMANCE_[a-zA-Z0-9_]+_\d{8}.json"

    def _validate_file(self):
        super()._validate_file()

        try:
            if not self.filename.endswith(".json"):
                raise ValueError("Invalid file extension. Check file")
            elif re.match(self.pattern, self.filename, re.IGNORECASE) is None:
                logger.warning("Invalid JSON filename pattern. Check file")
            else:
                logger.info(f"File {self.filename} passed validation check.")
        except Exception as error:
            logger.warning(f"File {self.filename} encountered: {error}.")

    def _read_source(self):
        with open(self.fullpath, "r") as f:
            return f.read()

    def _parse_records(self, raw):
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as error:
            logger.error(
                f"Failed to parse JSON for {self.filename}: {error}."
                f" Skipping file."
            )
            return None
        return obj

    def _postprocess_df(self, df):
        df = super()._postprocess_df(df)
        return df


class TextLoader(BaseLoader):

    required_columns = 4

    def __init__(self, fullpath):
        super().__init__(fullpath)
        self.filename = os.path.basename(self.fullpath)
        self.pattern = r"^CLICKSTREAMS_[a-zA-Z0-9_]+_\d{8}.txt"

    def _validate_file(self):
        super()._validate_file()

        try:
            if not self.filename.endswith(".txt"):
                raise ValueError("Invalid file extension. Check file")
            elif re.match(self.pattern, self.filename, re.IGNORECASE) is None:
                logger.warning("Invalid Text filename pattern. Check file")
            else:
                logger.info(f"File {self.filename} passed validation check.")
        except Exception as error:
            logger.warning(f"File {self.filename} encountered: {error}.")

    def _read_source(self):
        return super()._read_source()

    def _parse_records(self, raw):
        try:
            parsed_log_data = []

            for line in raw.splitlines():
                cleaned_line = line.strip()  # remove whitespace

                parts = cleaned_line.split(" | ")
                curr_log_entry = {}

                for part in parts:
                    if ": " in part:
                        key, value = part.split(": ", 1)
                        curr_log_entry[key.strip().lower()] = value.strip()
                    if len(parts) != self.required_columns:
                        raise ValueError(
                            f"Invalid text format in {self.filename}: {line}"
                        )

                    parsed_log_data.append(curr_log_entry)
            return parsed_log_data
        except Exception as error:
            logger.error(
                f"Failed to parse text for {self.filename}: {error}."
                f" Skipping file."
            )
            return None

    def _postprocess_df(self, df):
        df = super()._postprocess_df(df)
        if "date" in df.columns:
            df.loc[:, "date"] = pd.to_datetime(df["date"], format="%m/%d/%Y")
        return df
