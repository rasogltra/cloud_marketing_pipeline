from configparser import ConfigParser
from sqlalchemy import create_engine
import os, logging

logger = logging.getLogger(__name__)
_engine = None

def get_config():
        parser = ConfigParser()
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..'))
        path = os.path.join(root,'config' ,'config.ini')
        
        parser.read(path)
    
        if not parser.read(path):
            raise FileNotFoundError(f"config.ini file not found: {path}")
        return parser

def get_db_engine():
    global _engine
    if _engine is not None:
        return _engine
    
    config = get_config()
    
    db_params = {}
    if config.has_section('Postgresql'):
        for key, value in config.items('Postgresql'):
            db_params[key] = value
    else:
        logger.error("Database section not found in config file.")
        exit(1)
    
    try:
        connection_str = (
            f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}"
            f"@{db_params['host']}:5432/{db_params['database_name']}"
        )
        
        _engine = create_engine(connection_str)
        
        return _engine
    except Exception as error:
        logger.error(f"Unable to establish connection to database: {error}")