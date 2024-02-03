#fb_ad
#mkt_account
import requests
import pandas as pd
from datetime import  datetime, timedelta
import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'report-realtime-350003-c4cc0f514e7e.json'
client = bigquery.Client()

project_id = 'report-realtime-350003'
ad_table = client.get_table(client.dataset('Rossie').table('FB_Ad'))
report_table = client.get_table(client.dataset('Rossie').table('FB_AdReport'))

def exact(list): 
    result = []
    for i in list:
        if i is not None:
            result.append(i)
        else: pass
    if len(result) == 0:
        return 0
    else: return result[0]

def report_requests(account_id,token):
    host = "https://graph.facebook.com/v16.0/"
    parameter = "/insights?date_preset=last_3d&fields=account_id%2Caccount_name%2Ccampaign_id%2Ccampaign_name%2Cadset_id%2Cadset_name%2Cad_id%2Cad_name%2Caccount_currency%2Cclicks%2Cimpressions%2Creach%2Cspend%2Ccreated_time%2Cactions%2Ccost_per_unique_click&filtering=%5B%7Bfield%3A%22action_type%22%2C%22operator%22%3A%22IN%22%2C%22value%22%3A%5B%22onsite_conversion.messaging_first_reply%22%2C%22onsite_conversion.messaging_conversation_started_7d%22%2C%22comment%22%2C%22omni_complete_registration%22%2C%22landing_page_view%22%2C%22link_click%22%5D%7D%5D&level=ad&time_increment=1&limit=200&transport=cors&access_token="
    #Khi muốn chọn ngày copy đoạn sau lên parameter: &time_range={since:'2023-03-22',until:'2023-03-22'}
    url = host + account_id + parameter + token
    response = requests.request("GET", url)
    data = response.json()

    try:
        for item in data['data']:
            id = item['date_start'] + item['ad_id']
            if item.get('cost_per_unique_click'):
                if float(item['cost_per_unique_click']) != 0 or float(item['spend']):
                    clicks = round(float(item['spend'])/float(item['cost_per_unique_click']))
                else: clicks = 0
            else: clicks = 0
            row = id,\
                item['account_id'],\
                item['account_name'],\
                item['campaign_id'],\
                item['campaign_name'],\
                item['adset_id'],\
                item['adset_name'],\
                item['ad_id'],\
                item['ad_name'],\
                datetime.strptime(item['date_start'],'%Y-%m-%d').strftime('%Y-%m-%d'),\
                datetime.strptime(item['date_stop'],'%Y-%m-%d').strftime('%Y-%m-%d'),\
                item['account_currency'],\
                float(item['spend']),\
                int(item['impressions']) if item.get('impressions') else None,\
                int(item['reach']) if item.get('reach') else None,\
                clicks,\
                int(exact([x['value'] if x['action_type']=='onsite_conversion.messaging_first_reply' else None for x in item['actions']])) if item.get('actions') else 0,\
                int(exact([x['value'] if x['action_type']=='comment' else None for x in item['actions']])) if item.get('actions') else 0,\
                int(exact([x['value'] if x['action_type']=='omni_complete_registration' else None for x in item['actions']])) if item.get('actions') else 0,\
                int(exact([x['value'] if x['action_type']=='landing_page_view' else None for x in item['actions']])) if item.get('actions') else 0,\
                int(exact([x['value'] if x['action_type']=='link_click' else None for x in item['actions']])) if item.get('actions') else 0,\
                datetime.strptime(item['created_time'],'%Y-%m-%d').strftime('%Y-%m-%d'),\
                int(exact([x['value'] if x['action_type']=='onsite_conversion.messaging_conversation_started_7d' else None for x in item['actions']])) if item.get('actions') else 0
            
            row = [None if (val is None or val == '') else val for val in row]
            delete_job = client.query("DELETE FROM `report-realtime-350003.Rossie.FB_AdReport` WHERE id = '" + id + "'")
            delete_job.result()
            client.insert_rows(report_table, [row])
            
    except:
        try:
            with open('tracking.txt', 'a') as file:
                file.write('\n' + 'AdReport!!! ' + data['error']['message'] + '!!! ' + str(datetime.now()))
        except:
            with open('tracking.txt', 'a') as file:
                file.write('\n' + 'Lỗi khác')                  


account_list = client.query("SELECT id, token FROM `report-realtime-350003.Rossie.FB_AdAccount`")
for i in account_list:
    report_requests(i[0],i[1])


