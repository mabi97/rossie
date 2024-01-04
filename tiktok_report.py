import requests
import pandas as pd
from datetime import  datetime, timedelta
import os
from google.cloud import bigquery

def tiktok_request(advertiser_id,token,advertiser_name):
  start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d') 
  end_date =  datetime.now().strftime('%Y-%m-%d')
  url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/?advertiser_id=" + f'{advertiser_id}' + "&page_size=200&report_type=BASIC&dimensions=[\"ad_id\", \"stat_time_day\"]&data_level=AUCTION_AD&start_date="+ f'{start_date}' +"&end_date="+ f'{end_date}' +"&metrics=[\"ad_name\",\"adgroup_name\",\"campaign_name\",\"objective_type\",\"currency\",\"spend\",\"impressions\",\"reach\",\"clicks\",\"conversion\"]"
  payload={}
  headers = {
    'Access-Token': token,
    'Content-Type': 'application/json'
  }
  response = requests.request("GET", url, headers=headers, data=payload)
  data = response.json()

  #try: 
  for i in data['data']['list']:
      date = datetime.strptime(i['dimensions']['stat_time_day'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
      ad_id = i['dimensions']['ad_id']
      id = date + ad_id
      row = id,\
          date,\
          ad_id,\
          i['metrics']['ad_name'],\
          i['metrics']['adgroup_name'],\
          i['metrics']['campaign_name'],\
          i['metrics']['currency'],\
          float(i['metrics']['spend']),\
          int(i['metrics']['impressions']),\
          int(i['metrics']['reach']),\
          int(i['metrics']['clicks']),\
          int(i['metrics']['conversion']),\
          advertiser_name,\
          i['metrics']['objective_type']

      delete_job = client.query("DELETE FROM `report-realtime-350003.Rossie.Tiktok_AdReport` WHERE id = '" + id + "'")
      delete_job.result()
      client.insert_rows(ad_table, [row])
  #except: pass

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'report-realtime-350003-c4cc0f514e7e.json'
client = bigquery.Client()

project_id = 'report-realtime-350003'
ad_table = client.get_table(client.dataset('Rossie').table('Tiktok_AdReport'))



account_list = client.query("SELECT id, token, name FROM `report-realtime-350003.Rossie.Tiktok_AdAcount`")

for i in account_list:
    tiktok_request(i[0],i[1],i[2])

