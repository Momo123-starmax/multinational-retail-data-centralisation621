import pandas as pd
import numpy as np
import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    
    def __init__(self):
        pass

    def read_db_creds(self):
        with open("db_creds.yaml", "r") as f:
            db_creds = yaml.safe_load(f)
        return db_creds 

    def init_db_engine(self, db_creds):
        engine = create_engine(f"{db_creds['RDS_DATABASE_TYPE']}+{db_creds['DB_API']}://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        engine.connect()
        
        return engine

    def list_db_tables(self, engine):
        engine.connect()
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
       

    def upload_to_db(self, data_frame, table_name, db_creds):
        local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
        local_engine.connect()
        data_frame.to_sql(table_name, local_engine, if_exists='replace')


