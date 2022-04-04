from datetime import date
from google.cloud import bigquery
import re
import requests as rq
import pandas as pd
import json
from datetime import datetime as dt
import numpy as np


# In[8]:


SERVICE_ACCOUNT_JSON = r'your service account file path'
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
table_id = "table_id"
#dataset = bigquery.Dataset(dataset_id)
#dataset.location("US")

login = r'C:\pipelines\suporte\zendesk\bin\login_json.json'

with open(login) as f:
    data_login = json.load(f)
    zendesk_user = data_login['user']
    zendesk_pwd = data_login['pwd']


#print("Created dataset {}.{}".format(client.project, dataset_ref.dataset_id))


# In[130]:


page_id = 1

all_tickets =[]

while tickets_metrics['next_page'] != None:
    url_metrics = 'https://paylivrehelp.zendesk.com/api/v2/ticket_metrics.json?page='+str(page_id)
    response = rq.get(url_metrics, auth=(zendesk_user, zendesk_pwd))
    ticket_metrics = response.json()
    page_id += 1
    print(ticket_metrics['next_page'])
    next_page = ticket_metrics['next_page']
    if ticket_metrics['next_page'] == None:
        break
    else:
        lista = ticket_metrics['ticket_metrics']
        for i in range(len(lista)):
            one_ticket = []
            one_ticket.append(lista[i]['id'])
            one_ticket.append(lista[i]['ticket_id'])
            one_ticket.append(lista[i]['created_at'])
            one_ticket.append(lista[i]['updated_at'])
            one_ticket.append(lista[i]['group_stations'])
            one_ticket.append(lista[i]['assignee_stations'])
            one_ticket.append(lista[i]['reopens'])
            one_ticket.append(lista[i]['replies'])
            one_ticket.append(lista[i]['assignee_updated_at'])
            one_ticket.append(lista[i]['requester_updated_at'])
            one_ticket.append(lista[i]['status_updated_at'])
            one_ticket.append(lista[i]['initially_assigned_at'])
            one_ticket.append(lista[i]['assigned_at'])
            one_ticket.append(lista[i]['solved_at'])
            one_ticket.append(lista[i]['reply_time_in_minutes']['calendar'])
            one_ticket.append(lista[i]['reply_time_in_minutes']['business'])
            one_ticket.append(lista[i]['first_resolution_time_in_minutes']['calendar'])
            one_ticket.append(lista[i]['first_resolution_time_in_minutes']['business'])
            one_ticket.append(lista[i]['full_resolution_time_in_minutes']['calendar'])
            one_ticket.append(lista[i]['full_resolution_time_in_minutes']['business'])
            one_ticket.append(lista[i]['agent_wait_time_in_minutes']['calendar'])
            one_ticket.append(lista[i]['agent_wait_time_in_minutes']['business'])
            one_ticket.append(lista[i]['requester_wait_time_in_minutes']['calendar'])
            one_ticket.append(lista[i]['requester_wait_time_in_minutes']['business'])
            one_ticket.append(lista[i]['on_hold_time_in_minutes']['calendar'])
            one_ticket.append(lista[i]['on_hold_time_in_minutes']['business'])
            print(one_ticket)
            all_tickets.append(one_ticket)


# In[131]:


colunas = ['id','ticket_id','created_at','updated_at','group_stations','assignee_stations','reopens','replies','assignee_updated_at',
          'requester_updated_at','status_updated_at','initially_assigned_at','assigned_at','solved_at','reply_time_in_minutes_calendar',
           'reply_time_in_minutes_business','first_resolution_time_in_minutes_calendar','first_resolution_time_in_minutes_business',
           'full_resolution_time_in_minutes_calendar','full_resolution_time_in_minutes_business','agent_wait_time_in_minutes_calendar',
           'agent_wait_time_in_minutes_business','requester_wait_time_in_minutes_calendar','requester_wait_time_in_minutes_business',
           'on_hold_time_in_minutes_calendar','on_hold_time_in_minutes_business']

df_metrics = pd.DataFrame(all_tickets, columns=colunas)
df_metrics.head()



# In[132]:


pd.set_option("display.max_columns", None)
df_metrics.head(100)


# In[133]:


df_metrics = df_metrics.astype(str)
df_metrics['assignee_updated_at'] = df_metrics['assignee_updated_at'].replace('nan', '2100-01-1 00:00:01', regex=True)
df_metrics['assignee_updated_at'] = df_metrics['assignee_updated_at'].replace('NaN', '2100-01-1 00:00:01', regex=True)
df_metrics['assignee_updated_at'] = df_metrics['assignee_updated_at'].replace('None', '2100-01-1 00:00:01', regex=True)

df_metrics['requester_updated_at'] = df_metrics['requester_updated_at'].replace('nan', '2100-01-1 00:00:01', regex=True)
df_metrics['requester_updated_at'] = df_metrics['requester_updated_at'].replace('NaN', '2100-01-1 00:00:01', regex=True)
df_metrics['requester_updated_at'] = df_metrics['requester_updated_at'].replace('None', '2100-01-1 00:00:01', regex=True)

df_metrics['status_updated_at'] = df_metrics['status_updated_at'].replace('nan', '2100-01-1 00:00:01', regex=True)
df_metrics['status_updated_at'] = df_metrics['status_updated_at'].replace('NaN', '2100-01-1 00:00:01', regex=True)
df_metrics['status_updated_at'] = df_metrics['status_updated_at'].replace('None', '2100-01-1 00:00:01', regex=True)

df_metrics['initially_assigned_at'] = df_metrics['initially_assigned_at'].replace('nan', '2100-01-1 00:00:01', regex=True)
df_metrics['initially_assigned_at'] = df_metrics['initially_assigned_at'].replace('NaN', '2100-01-1 00:00:01', regex=True)
df_metrics['initially_assigned_at'] = df_metrics['initially_assigned_at'].replace('None', '2100-01-1 00:00:01', regex=True)

df_metrics['assigned_at'] = df_metrics['assigned_at'].replace('nan', '2100-01-1 00:00:01', regex=True)
df_metrics['assigned_at'] = df_metrics['assigned_at'].replace('NaN', '2100-01-1 00:00:01', regex=True)
df_metrics['assigned_at'] = df_metrics['assigned_at'].replace('None', '2100-01-1 00:00:01', regex=True)

df_metrics['solved_at'] = df_metrics['solved_at'].replace('nan', '2100-01-1 00:00:01', regex=True)
df_metrics['solved_at'] = df_metrics['solved_at'].replace('NaN', '2100-01-1 00:00:01', regex=True)
df_metrics['solved_at'] = df_metrics['solved_at'].replace('None', '2100-01-1 00:00:01', regex=True)

df_metrics = df_metrics.replace('nan', '', regex=True)
df_metrics = df_metrics.replace('NaN', '', regex=True)
df_metrics = df_metrics.replace('None', '', regex=True)
df_metrics['created_at'] = df_metrics['created_at'].str.replace('Z', '')
df_metrics['updated_at'] = df_metrics['updated_at'].str.replace('Z', '')
df_metrics['assignee_updated_at'] = df_metrics['assignee_updated_at'].str.replace('Z', '')
df_metrics['requester_updated_at'] = df_metrics['requester_updated_at'].str.replace('Z', '')
df_metrics['status_updated_at'] = df_metrics['status_updated_at'].str.replace('Z', '')
df_metrics['initially_assigned_at'] = df_metrics['initially_assigned_at'].str.replace('Z', '')
df_metrics['assigned_at'] = df_metrics['assigned_at'].str.replace('Z', '')
df_metrics['solved_at'] = df_metrics['solved_at'].str.replace('Z', '')
df_metrics['extraction_date'] = dt.now().strftime("%Y-%m-%d %H:%M:%S")
df_metrics['extraction_date'] = df_metrics['extraction_date'].str.replace(' ', 'T')
df_metrics['created_at'] = df_metrics['created_at'].str.replace(' ', 'T')
df_metrics['updated_at'] = df_metrics['updated_at'].str.replace(' ', 'T')
df_metrics['assignee_updated_at'] = df_metrics['assignee_updated_at'].str.replace(' ', 'T')
df_metrics['requester_updated_at'] = df_metrics['requester_updated_at'].str.replace(' ', 'T')
df_metrics['status_updated_at'] = df_metrics['status_updated_at'].str.replace(' ', 'T')
df_metrics['initially_assigned_at'] = df_metrics['initially_assigned_at'].str.replace(' ', 'T')
df_metrics['assigned_at'] = df_metrics['assigned_at'].str.replace(' ', 'T')
df_metrics['solved_at'] = df_metrics['solved_at'].str.replace(' ', 'T')
#df_metrics['extraction_date'] = df_metrics['extraction_date'].astype(str).str[:10] + 'T' + df_metrics['extraction_date'].astype(str).str[10:]
df_metrics['extraction_date'] = pd.to_datetime(df_metrics['extraction_date'])
df_metrics['created_at'] = pd.to_datetime(df_metrics['created_at'])
df_metrics['updated_at'] = pd.to_datetime(df_metrics['updated_at'])
df_metrics['assignee_updated_at'] = pd.to_datetime(df_metrics['assignee_updated_at'])
df_metrics['requester_updated_at'] = pd.to_datetime(df_metrics['requester_updated_at'])
df_metrics['status_updated_at'] = pd.to_datetime(df_metrics['status_updated_at'])
df_metrics['initially_assigned_at'] = pd.to_datetime(df_metrics['initially_assigned_at'])
df_metrics['assigned_at'] = pd.to_datetime(df_metrics['assigned_at'])
df_metrics['solved_at'] = pd.to_datetime(df_metrics['solved_at'])
df_metrics['ticket_id'] = df_metrics['ticket_id'].astype(np.int64)
df_metrics['id'] = df_metrics['id'].astype(np.int64)
df_metrics['reopens'] = df_metrics['reopens'].astype(np.int64)
df_metrics['replies'] = df_metrics['replies'].astype(np.int64)


# In[134]:


df_metrics.dtypes


# In[135]:


job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField(name="id", field_type="INT64"),
        bigquery.SchemaField(name="ticket_id", field_type="INT64"),
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField(name="created_at", field_type="DATETIME"),
        bigquery.SchemaField(name="updated_at", field_type="DATETIME"),
        bigquery.SchemaField(name="group_stations", field_type="STRING"),
        bigquery.SchemaField(name="assignee_stations", field_type="STRING"),
        bigquery.SchemaField(name="reopens", field_type="INT64"),
        bigquery.SchemaField(name="replies", field_type="INT64"),
        bigquery.SchemaField(name="assignee_updated_at", field_type="DATETIME"),
        bigquery.SchemaField(name="requester_updated_at", field_type="DATETIME"),
        bigquery.SchemaField(name="status_updated_at", field_type="DATETIME"),
        bigquery.SchemaField(name="initially_assigned_at", field_type="DATETIME"),
        bigquery.SchemaField(name="assigned_at", field_type="DATETIME"),
        bigquery.SchemaField(name="solved_at", field_type="DATETIME"),
        bigquery.SchemaField(name="reply_time_in_minutes_calendar", field_type="STRING"),
        bigquery.SchemaField(name="reply_time_in_minutes_business", field_type="STRING"),
        bigquery.SchemaField(name="first_resolution_time_in_minutes_calendar", field_type="STRING"),
        bigquery.SchemaField(name="first_resolution_time_in_minutes_business", field_type="STRING"),
        bigquery.SchemaField(name="full_resolution_time_in_minutes_calendar", field_type="STRING"),
        bigquery.SchemaField(name="full_resolution_time_in_minutes_business", field_type="STRING"),
        bigquery.SchemaField(name="agent_wait_time_in_minutes_calendar", field_type="STRING"),
        bigquery.SchemaField(name="agent_wait_time_in_minutes_business", field_type="STRING"),
        bigquery.SchemaField(name="requester_wait_time_in_minutes_calendar", field_type="STRING"),
        bigquery.SchemaField(name="requester_wait_time_in_minutes_business", field_type="STRING"),
        bigquery.SchemaField(name="on_hold_time_in_minutes_calendar", field_type="STRING"),
        bigquery.SchemaField(name="on_hold_time_in_minutes_business", field_type="STRING"),
        bigquery.SchemaField(name="extraction_date", field_type="DATETIME")
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)


# In[136]:


job = client.load_table_from_dataframe(
    df_metrics, table_id, job_config=job_config
)


# In[137]:


job.result()


# In[138]:


update_metrics = """UPDATE `table_id`
SET assignee_updated_at = NULL
WHERE assignee_updated_at = '2100-01-01T00:00:01';
UPDATE `table_id`
SET requester_updated_at = NULL
WHERE requester_updated_at = '2100-01-01T00:00:01';
UPDATE `table_id`
SET status_updated_at = NULL
WHERE status_updated_at = '2100-01-01T00:00:01';
UPDATE `table_id`
SET initially_assigned_at = NULL
WHERE initially_assigned_at = '2100-01-01T00:00:01';
UPDATE `table_id`
SET assigned_at = NULL
WHERE assigned_at = '2100-01-01T00:00:01';
UPDATE `table_id`
SET solved_at = NULL
WHERE solved_at = '2100-01-01T00:00:01';
UPDATE `table_id`
SET reply_time_in_minutes_calendar = NULL
WHERE reply_time_in_minutes_calendar = '';
UPDATE `table_id`
SET reply_time_in_minutes_business = NULL
WHERE reply_time_in_minutes_business = '';
UPDATE `table_id`
SET first_resolution_time_in_minutes_calendar = NULL
WHERE first_resolution_time_in_minutes_calendar = '';
UPDATE `table_id`
SET first_resolution_time_in_minutes_business = NULL
WHERE first_resolution_time_in_minutes_business = '';
UPDATE `table_id`
SET full_resolution_time_in_minutes_calendar = NULL
WHERE full_resolution_time_in_minutes_calendar = '';
UPDATE `table_id`
SET full_resolution_time_in_minutes_business = NULL
WHERE full_resolution_time_in_minutes_business = '';
UPDATE `table_id`
SET agent_wait_time_in_minutes_calendar = NULL
WHERE agent_wait_time_in_minutes_calendar = '';
UPDATE `table_id`
SET agent_wait_time_in_minutes_business = NULL
WHERE agent_wait_time_in_minutes_business = '';
UPDATE `table_id`
SET requester_wait_time_in_minutes_calendar = NULL
WHERE requester_wait_time_in_minutes_calendar = '';
UPDATE `table_id`
SET requester_wait_time_in_minutes_business = NULL
WHERE requester_wait_time_in_minutes_business = '';
UPDATE `table_id`
SET on_hold_time_in_minutes_calendar = NULL
WHERE on_hold_time_in_minutes_calendar = '';
UPDATE `table_id`
SET on_hold_time_in_minutes_business = NULL
WHERE on_hold_time_in_minutes_business = '';"""

query_job = client.query(update_metrics)


# In[ ]:




