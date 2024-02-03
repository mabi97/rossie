import requests
import pandas as pd
from datetime import  datetime, timedelta
import os
from google.cloud import bigquery


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'report-realtime-350003-c4cc0f514e7e.json'
client = bigquery.Client()

table_id = "report-realtime-350003.Rossie.Test"

current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

sql_query = f"""
    INSERT INTO {table_id} (time)
    VALUES ('{current_time}')
"""

query_job = client.query(sql_query)

query_job.result()

