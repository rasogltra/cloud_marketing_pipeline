#!/usr/bin/env python
# coding: utf-8
# etl.py

import pandas as pd
import os,re,json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class BaseLoader:
    def __init__(self, file, fullpath):
        self.file = file
        self.fullpath = fullpath
        
    def _validate_file(self):
        if not os.path.exists(self.fullpath):
            logger.error(f"File {self.file} doesn't exists. Check folders. ")
            raise FileNotFoundError(f"File {self.file} in path {self.fullpath} not found.")
                
    def _read_source(self):
        with open(self.fullpath, 'r') as f1:
            content = f1.read()
            return content
            
    def _parse_records(self, raw):
        raise NotImplementedError("Subclasses must implement method.")
    
    def _to_dataframe(self, records):
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
                raise ValueError("List elements are neither all Dataframes nor all dict-like objects.")
        else:
            raise TypeError("Unsupported type for records in _to_dataframe")    
                    
    def _postprocess_df (self, df):
        df = df.drop_duplicates()
        return df
    
    def load(self):
        self._validate_file()
        raw = self._read_source()
        records = self._parse_records(raw)
        df = self._to_dataframe(records)
        df = self._postprocess_df(df)
        return df
            
class CSVLoader(BaseLoader):
    def __init__(self, file, fullpath, delimiter=","):
        super().__init__(file, fullpath) # Call parent's __init__
        self.delimiter = delimiter
    
    def _validate_file(self):
        super()._validate_file()
        
        filename= os.path.basename(self.fullpath)
        pattern= r'^[a-zA-Z0-9_]+_\d{8}(\..+)?$'
        req_columns = ['Date', 'Channel', 'Spend_usd', 'Client']
        
        if not self.fullpath.lower().endswith(".csv"):
            raise ValueError(f"Expected .csv file, got: {filename}")
        
        if re.match(pattern, filename) is None:
            logger.warning(f"Filename pattern mismatch for {filename}. Check filename.")
            
        try:
            header = pd.read_csv(self.fullpath, nrows=0) # get header row only
            col_names = list(header.columns)
        except Exception as error:
            logger.warning(f"File {filename} encountered a header error: {error}.")
        
        if not all (col in col_names for col in req_columns):
            logger.warning("CSV file {filename} is missing the required columns. Skipping file.")
        else:
            logger.info(f"File {filename} passed validation check.")
 
    def _read_source(self):
        return pd.read_csv(self.fullpath, sep=self.delimiter)
    
    def _parse_records(self, raw):
        return raw
    
    def _postprocess_df(self, df):
        df = super()._postprocess_df(df)
        df.columns = df.columns.str.lower()
        return df
    
class JSONLoader (BaseLoader):
    def __init__(self, file, fullpath):
        super().__init__(file, fullpath)
    
    def _validate_file(self):
        super()._validate_file()
        
        filename = os.path.basename(self.fullpath)
        
        if not self.fullpath.lower().endswith(".json"):
            raise ValueError(f"Expected .json file, got: {filename}")
        else:
            logger.info(f"File {filename} passed validation check.")
            
    def _read_source(self):
        with open(self.fullpath, 'r') as f:
            return f.read()
    
    def _parse_records(self, raw):
        try:
           obj = json.loads(raw)
        except json.JSONDecodeError as error:
            logger.error(f"Invalid JSON format in {self.file}: {error}. Skipping file.")
            return None
        return obj
    
    def _postprocess_df(self, df):
        df = super()._postprocess_df(df)
        return df

class TextLoader (BaseLoader):
    def __init__(self, file, fullpath):
        super().__init__(file, fullpath)
    
    def _validate_file(self):
        super()._validate_file()
        
        filename = os.path.basename(self.fullpath)
        pattern = r'^[a-zA-Z0-9_]+_\d{8}(\..+)?$'
        
        if not self.fullpath.lower().endswith(".txt"):
            raise ValueError(f"Expected .txt file, got: {filename}")
        elif re.match(pattern, filename) is None:
            logger.warning(f"Filename pattern mismatch for {filename}. Check filename.")
        else:
            logger.info(f"File {filename} passed validation check.")
            
    def _read_source(self):
        return super()._read_source()   
    
    def _parse_records(self, raw):
        try:
            required_cols = ['client', 'date', 'channel', 'event'] 
            parsed_log_data = [] 
             
            for line in raw.splitlines():
                cleaned_line = line.strip() # remove whitespace
                
                parts = cleaned_line.split(' | ')
                curr_log_entry = {}
                
                for part in parts:
                    if ': ' in part:
                        key, value = part.split(': ', 1)
                        curr_log_entry[key.strip().lower()] = value.strip()
                        
                if all(col in curr_log_entry for col in required_cols): 
                    parsed_log_data.append(curr_log_entry)
                else:
                    logger.warning(f"Incomplete log entry skipped: {curr_log_entry}") 
                    
            return parsed_log_data 
        except Exception as error:
            logger.warning(f"Invalid text file. Skipping file: {error}")
            return None
        
    def _postprocess_df(self, df):
        df = super()._postprocess_df(df)
        if 'date' in df.columns:
            df.loc[:,'date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')
        return df