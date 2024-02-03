import requests
import pandas as pd
import pandas_gbq
from datetime import  datetime, timedelta, timezone
import os
from google.cloud import bigquery

product_list = ["~oliadisbho","~oliadis","~oliadibho","~oliadi","~rosmibho","~rosmi","~rosupbho","~rosup","~rosepbho","~rose","~vtnt","~herlip7bho","~herlip7",
                "~loudikbhho","~loudik"]
bho_list = ["bot#","1#","bho#","2#","mau#","ly#","gia#","tc#","nt#","kt#","x#","si#",
            "tnp1$","tnp2$","tnp3$","tnp4$","tnp5$","tnp6$","tnp7$","tnp8$","tnp9$"]

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'report-realtime-350003-c4cc0f514e7e.json'
client = bigquery.Client()

product_max_time = client.query("select max(inserted_time) as max_inserted_time from `report-realtime-350003.Rossie.Pancake_Inbox_Product`")
for i in product_max_time:
    product_max_time_value = i[0].replace(tzinfo=None)

tag_max_time = client.query("select max(inserted_time) as max_inserted_time from `report-realtime-350003.Rossie.Pancake_Inbox_Tag`")
for i in tag_max_time:
    tag_max_time_value = i[0].replace(tzinfo=None)
    tag_max_time_value

tag_max_time = client.query("select max(inserted_time) as max_inserted_time from `report-realtime-350003.Rossie.Pancake_Inbox_Assign`")
for i in tag_max_time:
    assign_max_time_value = i[0].replace(tzinfo=None)


def split(string,list_check):
    for i in list_check:
        if i in string.lower():
            return i
            break
         
def get_message_data(conversation_id,customer_id,page_id,token):
    url_1 = "https://pages.fm/api/public_api/v1/pages/"+f'{page_id}'+"/conversations/"+f'{conversation_id}'+"/messages?access_token="+f'{token}'+"&customer_id="+f'{customer_id}'+"&conversation_id="+f'{conversation_id}'+"&page_id="+f'{page_id}'
    url_2 = "https://pages.fm/api/public_api/v1/pages/"+f'{page_id}'+"/conversations/"+f'{conversation_id}'+"/messages?access_token="+f'{token}'+"&customer_id="+f'{customer_id}'+"&conversation_id="+f'{conversation_id}'+"&page_id="+f'{page_id}'+"&current_count=25"
    url_3 = "https://pages.fm/api/public_api/v1/pages/"+f'{page_id}'+"/conversations/"+f'{conversation_id}'+"/messages?access_token="+f'{token}'+"&customer_id="+f'{customer_id}'+"&conversation_id="+f'{conversation_id}'+"&page_id="+f'{page_id}'+"&current_count=50"
    all_message = requests.request("GET", url_3).json().get('messages') + requests.request("GET", url_2).json().get('messages') + requests.request("GET", url_1).json().get('messages')
    product = None
    df = []
    for i in all_message:
        if "~" in i.get('message'):
            if split(i.get('message'),product_list) != None:
                product = split(i.get('message'),product_list)
                insert_time = datetime.strptime(i.get('inserted_at'), '%Y-%m-%dT%H:%M:%S.%f') + timedelta(hours=7)
                if insert_time > product_max_time_value:
                    row = [i.get('conversation_id') + str(insert_time), i.get('conversation_id'), i.get('page_id'), insert_time, product]
                    df.append(row)
    
    return df

      
page_list = client.query("""
                        select id, token, name
                        from `report-realtime-350003.Rossie.FB_Page`
                        where token is not null
                        """)

since = int((datetime.now()-timedelta(hours=3)).timestamp())
until = int((datetime.now()).timestamp())

product_df = []
tag_df = []
assign_df = []

for i in page_list:
    page_id = i[0]
    token = i[1]
    name = i[2]
    url = "https://pages.fm/api/public_api/v1/pages/"+f'{page_id}'+"/conversations?since="+f'{since}'+"&until="+f'{until}'+"&page_number=1&access_token="+f'{token}'+"&order_by=updated_at&page_id="+f'{page_id}'
    response = requests.request("GET", url)
    data = response.json()

    if data.get('conversations') is not None:
        for i in data.get('conversations'):
            if i.get('page_id') in i.get('id'):
                conversation_id = i.get('id')
                customer_id = i.get('customers')[0].get('id')
                #product_df.extend(get_message_data(conversation_id,customer_id,page_id,token))

                tag_data = i.get('tag_histories')
                for x in tag_data:
                    inserted_at = datetime.strptime(x.get('inserted_at'), '%Y-%m-%dT%H:%M:%S') + timedelta(hours=7)
                    action = x.get('payload').get('action')
                    tag_name = x.get('payload').get('tag').get('text')
                    if inserted_at > tag_max_time_value:
                        tag_df.append([conversation_id + x.get('inserted_at'), conversation_id, inserted_at, action, tag_name])
                
                assign_data = i.get('assignee_histories')
                for y in assign_data:
                    inserted_at = datetime.strptime(y.get('inserted_at'), '%Y-%m-%dT%H:%M:%S') + timedelta(hours=7)
                    assign_to = y.get('payload').get('added_users')[0].get('name')
                    if inserted_at > assign_max_time_value:
                        assign_df.append([conversation_id + y.get('inserted_at'), conversation_id, inserted_at, assign_to])


#product_df = pd.DataFrame(product_df, columns=['id', 'conversation_id', 'page_id', 'inserted_time', 'product'])
tag_df = pd.DataFrame(tag_df, columns=['id', 'conversation_id', 'inserted_time', 'action', 'tag'])
assign_df = pd.DataFrame(assign_df, columns=['id', 'conversation_id', 'inserted_time', 'assign_to'])

#pandas_gbq.to_gbq(product_df, 'report-realtime-350003.Rossie.Pancake_Inbox_Product', if_exists='append')
pandas_gbq.to_gbq(tag_df, 'report-realtime-350003.Rossie.Pancake_Inbox_Tag', if_exists='append')
pandas_gbq.to_gbq(assign_df, 'report-realtime-350003.Rossie.Pancake_Inbox_Assign', if_exists='append')              
