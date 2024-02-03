import requests
import pandas as pd
import os
import os
from google.cloud import bigquery
import pandas_gbq

url = "https://pages.fm/api/v1/pages?access_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1aWQiOiI3MzIzNDhhZi05NjVkLTRhYjUtYmE1MS1jNzk2YjhkYWNlY2UiLCJzZXNzaW9uX2lkIjoiSzdydGJ2RHk0RTlCSHpJVHk0QlJ5dytlYy9YZU9OdFVvUUlvVE56MEhMayIsIm5hbWUiOiJUcuG6p24gxJDhu6ljIE3huqFuaCIsImxvZ2luX3Nlc3Npb24iOm51bGwsImlhdCI6MTcwNjExMDIwMywiZmJfbmFtZSI6IlRy4bqnbiDEkOG7qWMgTeG6oW5oIiwiZmJfaWQiOiI1MDkyNzk2ODkyNzg4MjgiLCJleHAiOjE3MTM4ODYyMDMsImFwcGxpY2F0aW9uIjoxfQ.ISWFTjvsia75qse9yTJbUNom2btj2Yc-CKOgupiMByo"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

data = response.json().get('categorized').get('activated')

df = []

for i in data:
    row = [i.get('id'), i.get('name'), i.get('settings').get('page_access_token')]
    df.append(row)

df = pd.DataFrame(df, columns = ['id', 'name', 'token'])

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'report-realtime-350003-c4cc0f514e7e.json'
client = bigquery.Client()
table_id = 'report-realtime-350003.Rossie.FB_Page'

pandas_gbq.to_gbq(df, table_id, if_exists='replace')