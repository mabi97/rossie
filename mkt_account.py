 #mkt_account
import requests
import pandas as pd
from datetime import  datetime, timedelta
import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'report-realtime-350003-c4cc0f514e7e.json'
client = bigquery.Client()

project_id = 'report-realtime-350003'
fb_table = client.get_table(client.dataset('Rossie').table('FB_AdAccount'))
tiktok_table = client.get_table(client.dataset('Rossie').table('Tiktok_AdAcount'))

fb_token_list = ["EAAI9vVVEJgwBO1ZCqiFzWcAZCiMNe1ZCaHFtvZB1XpEVVjc7EZBMqNjS2d4UJ0GLnyUXxAwOIhKzYaZCoI2im2s07PXqpj9TZCuYz4Tkt9bPD3YbibJK2ZArs1hW9fZAlZC6calaOZCF7lZATFl1UbSZAIbdN91ZBUt1IMrYKyZAAADbWevs5XWtbg3d54MfHd0"]

tiktok_token_list = ["6f89bf3165824a253d73d09225b0bbcc17d6eb88","96f5da151feb42713ac1f4a3fdc6e664c13f9a35"]

def fb_adaccount_requests(token):
    host = "https://graph.facebook.com/v16.0/me?fields=adaccounts%7Bbalance%2Camount_spent%2Ccurrency%2Cspend_cap%2Cfunding_source_details%2Caccount_status%2Cname%7D%2Cid%2Cname&limit=100&access_token="
    url = host + token
    response = requests.request("GET", url)
    data = response.json()

    try:
        for item in data['adaccounts']['data']:
            id = item['id']
            try: 
                bank_account = item['funding_source_details']['display_string']
            except:
                bank_account = None
            row = item['id'],\
                float(item['balance']),\
                float(item['amount_spent']),\
                item['currency'],\
                float(item['spend_cap']),\
                str(item['account_status']),\
                data['name'],\
                token,\
                item['name'],\
                bank_account
            row = [None if (val is None or val == '') else val for val in row]
            delete_job = client.query("DELETE FROM `report-realtime-350003.Rossie.FB_AdAccount` WHERE id = '" + id + "'")
            delete_job.result()
            client.insert_rows(fb_table, [row])

    except: 
        try:
            with open('tracking.txt', 'a') as file:
                file.write('\n' + 'AdAccount!!! ' + data['error']['message'] + '!!! ' + str(datetime.now()))
        except: 
            with open('tracking.txt', 'a') as file:
                file.write('\n' + 'Lỗi code ' + str(datetime.now()))


def tiktok_adaccount_requests(token):

  url = "https://business-api.tiktok.com/open_api/v1.3/oauth2/advertiser/get/?app_id=7215062312636383234&secret=48628c61709f4ac9bd6b583b35f061370508c8fa"

  headers = {
    'Access-Token': token
  }

  response = requests.request("GET", url, headers=headers)
  data = response.json()

  for i in data['data']['list']:
      id = i['advertiser_id']
      row = id, i['advertiser_name'], token
      delete_job = client.query("DELETE FROM `report-realtime-350003.Rossie.Tiktok_AdAcount` WHERE id = '" + id + "'")
      delete_job.result()
      client.insert_rows(tiktok_table, [row])



for i in fb_token_list:
    fb_adaccount_requests(i)

for i in tiktok_token_list:
    tiktok_adaccount_requests(i)