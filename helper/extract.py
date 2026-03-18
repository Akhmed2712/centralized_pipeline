from utils.db import get_connection
import pandas as pd

def extract_source(source):
    if source["type"] == "mysql":
        conn = get_connection(
            source["connection"],
            env=source.get("env", "dev")
        )
        
        try:
            df = pd.read_sql(source["query"], conn)
            return df
        finally:
            conn.close()