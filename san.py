import requests
import pandas as pd
from datetime import  datetime, timedelta
import os
from google.cloud import bigquery

def main_requests(page):
    time = int((datetime.now()-timedelta(minutes=120)).timestamp())
    url = "https://pos.pages.fm/api/v1/shops/5213930/orders?api_key=e5b85d9090144a678ea1d63453c919b5&updateStatus=updated_at&page_size=100"
    response = requests.request("GET", url + f'&page_number={page}' + f'&startDateTime={time}')
    return response.json()

def get_pages(response):
    return response['total_pages']

def exact(list):
    result = []
    for i in list:
        if i is not None:
            result.append(i)
        else: pass
    if len(result) == 0:
        return None
    else: return result[0]

def exact_date(date):
    if date is None:
        return None
    else:
        return (datetime.strptime(date,  '%Y-%m-%dT%H:%M:%S') + timedelta(hours=7)).strftime('%Y-%m-%d')

def get_orders(response):

    for item in response['data']:
        id = str(item['id'])
        char =      str(item['id']),\
                    (datetime.strptime(item['updated_at'], '%Y-%m-%dT%H:%M:%S.%f') + timedelta(hours=7)).isoformat(),\
                    exact_date(exact([x['updated_at'] if x['status']==0 else None for x in item['status_history']])),\
                    item['status'],\
                    item['status_name'],\
                    item['order_sources_name'],\
                    item['account_name'],\
                    item['total_price'],\
                    item['shipping_fee'],\
                    item['shipping_address']['full_name'] if item['shipping_address'] is not None else None,\
                    item['shipping_address']['full_address'] if item['shipping_address'] is not None else None,\
                    item['shipping_address']['province_name'],\
                    item['partner']['extend_code'] if item['partner'] is not None else None,\
                    item['partner']['partner_name'] if item['partner'] is not None else None,\
                    exact_date(exact([x['updated_at'] if x['status']==1 else None for x in item['status_history']])),\
                    exact_date(exact([x['updated_at'] if x['status']==2 else None for x in item['status_history']])),\
                    str(item['bill_phone_number']),\
                    item['fee_marketplace'],\
                    item ['total_discount']
        
        char = [None if (val is None or val == '') else val for val in char]
        print(char)
        delete_job = client.query("DELETE FROM `report-realtime-350003.Rossie.Pan_Ecommerce_Orders` WHERE id = '" + id + "'")
        delete_job.result()
        client.insert_rows(table, [char])
            
        for i in item['items']: 
            order_detail_id = str(i['id'])
            row = str(i['id']),\
                id,\
                i['variation_info']['name'],\
                i['variation_info']['detail'],\
                i['quantity'],\
                i['variation_info']['retail_price'],\
                i['discount_each_product']        
            row = [None if (val is None or val == '') else val for val in row]
            delete_job = client.query("DELETE FROM `report-realtime-350003.Rossie.Pan_Ecommerce_Orders_Detail` WHERE id = '" + order_detail_id + "'")
            delete_job.result()
            client.insert_rows(detail_table, [row])
                

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'report-realtime-350003-c4cc0f514e7e.json'
client = bigquery.Client()

project_id = 'report-realtime-350003'
table = client.get_table(client.dataset('Rossie').table('Pan_Ecommerce_Orders'))
detail_table = client.get_table(client.dataset('Rossie').table('Pan_Ecommerce_Orders_Detail'))


data = main_requests(1)
for i in range(1,get_pages(data)+1):
    get_orders(main_requests(i))
