import requests
import pandas as pd
from datetime import  datetime, timedelta
import os
from google.cloud import bigquery

def main_requests(page):
    time = int((datetime.now()-timedelta(minutes=140)).timestamp())
    url = "https://pos.pages.fm/api/v1/shops/5118364/orders?api_key=ba7023239ae44f8db8c38591fdb5cf1a&updateStatus=updated_at&page_size=100"
    response = requests.request("GET", url + f'&page_number={page}' + f'&startDateTime={time}')
    return response.json()

def deleted_order_requests():
    time = int((datetime.now()-timedelta(minutes=140)).timestamp())
    url = "https://pos.pages.fm/api/v1/shops/5118364/orders?api_key=ba7023239ae44f8db8c38591fdb5cf1a&updateStatus=updated_at&page_size=200&status=7"
    response = requests.request("GET", url + f'&startDateTime={time}')
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
        return (datetime.strptime(date,  '%Y-%m-%dT%H:%M:%S') + timedelta(hours=7)).isoformat()

demand_list = ['IB','xng','xn2dt','xn1dt','xndt','xns','xn3','xn2','xn1','xn','cb2','cb1','call','tr','cb','ut']
cskh_list = ['HOTLINE','KH Mới','KQL2','KQL1','KQL']
failed_reason_list = ['Không tiếp cận', 'Giá', 'Sản phẩm']

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'report-realtime-350003-c4cc0f514e7e.json'
client = bigquery.Client()

# Specify your BigQuery project ID, dataset ID, and table ID
project_id = 'report-realtime-350003'
order_table = client.get_table(client.dataset('Rossie').table('Pan_Orders'))
order_detail_table = client.get_table(client.dataset('Rossie').table('Pan_Orders_Detail'))

def get_orders(response):
    for item in response['data']:
        id = str(item['id'])     
        char =      str(item['id']),\
                    (datetime.strptime(item['updated_at'], '%Y-%m-%dT%H:%M:%S.%f') + timedelta(hours=7)).isoformat(),\
                    (datetime.strptime([x['updated_at'] for x in item['status_history'] if x['status']==0][0], '%Y-%m-%dT%H:%M:%S') + timedelta(hours=7)).isoformat(),\
                    exact([x['name'] if x['status']==0 else None for x in item['status_history']]),\
                    item['status'],\
                    item['order_sources_name'],\
                    str(item['page_id']) if item['page_id'] is not None else None,\
                    item['bill_full_name'],\
                    item['bill_phone_number'],\
                    item['total_price'],\
                    item['shipping_fee'],\
                    item['ad_id'],\
                    item['shipping_address']['province_name'],\
                    exact([x['name'] if x['status']==1 else None for x in item['status_history']]),\
                    item['partner']['partner_name'] if item['partner'] is not None else None,\
                    item['partner']['extend_code'] if item['partner'] is not None else None,\
                    exact_date(exact([x['updated_at'] if x['status']==1 else None for x in item['status_history']])) ,\
                    exact_date(exact([x['updated_at'] if x['status']==2 else None for x in item['status_history']])) ,\
                    item['shipping_address']['full_address'] if item['shipping_address'] is not None else None,\
                    item['shipping_address']['full_name'] if item['shipping_address'] is not None else None,\
                    item['partner_fee'],\
                    item['total_discount'],\
                    ', '.join(item['customer_needs']) if item.get('customer_needs') else None,\
                    item['account_name'],\
                    exact_date(item['time_assign_seller']),\
                    item['assigning_seller']['name'] if item['assigning_seller'] is not None else None,\
                    exact_date(item['time_assign_care']),\
                    item['marketer']['name'] if item['marketer'] is not None else None,\
                    ', '.join(x['name'] for x in item['tags']) if item['tags'] != [] else None,\
                    exact([x['name'] if x['name'] in demand_list else None for x in item.get('tags')]),\
                    exact([x['name'] if x['name'] in cskh_list else None for x in item.get('tags')]),\
                    exact([x['name'] if x['name'] in failed_reason_list else None for x in item.get('tags')]),\
                    item['customer']['date_of_birth'],\
                    item['cod']
        try: 
            char = [None if (val is None or val == '') else val for val in char]
            delete_job = client.query("DELETE FROM `report-realtime-350003.Rossie.Pan_Orders` WHERE id = '" + id + "'")
            delete_job.result()
            client.insert_rows(order_table, [char])
        except: 
            with open('tracking.txt', 'a') as file:
                file.write('\n' + 'Order!!! ' + char + ' time_run:' + str(datetime.now()))         


        for i in item['items']: 
            order_detail_id = str(i['id'])
            row = str(i['id']),\
                id,\
                i['variation_info']['name'],\
                i['variation_info']['product_display_id'],\
                i['quantity'],\
                i['variation_info']['retail_price'],\
                i['discount_each_product']        
            try:
                row = [None if (val is None or val == '') else val for val in row]
                delete_job = client.query("DELETE FROM `report-realtime-350003.Rossie.Pan_Orders_Detail` WHERE id = '" + order_detail_id + "'")
                delete_job.result()
                client.insert_rows(order_detail_table, [row])
            except:
                pass

for i in range(1,get_pages(main_requests(1))+1):
    get_orders(main_requests(i))

get_orders(deleted_order_requests())