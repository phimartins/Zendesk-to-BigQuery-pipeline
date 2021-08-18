#!/usr/bin/env python
# coding: utf-8

# In[71]:


from google.cloud import bigquery
SERVICE_ACCOUNT_JSON = r'C:\pipelines\auth\paylivre-304215-78a2e9fec1be.json'
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
table_id = "paylivre-304215.suporte.zendesk_tickets"


# In[72]:


import logging
import os
from shutil import copy
from datetime import datetime as dt
import re
import requests as rq
import json
import pandas as pd
from urllib.parse import urlencode
import numpy as np
import time


class LoggingControl:
    def __init__(self, path, level=logging.INFO, logfmt=None, datefmt=None):
        self._path = path
        self._level = level
        self.__config(logfmt, datefmt)
        # logging levels
        self.DEBUG = logging.DEBUG
        self.INFO = logging.INFO
        self.WARNING = logging.WARNING
        self.ERROR = logging.ERROR
        self.CRITICAL = logging.CRITICAL
    def __config(self, logfmt, datefmt):
        if logfmt is None:
            logfmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s' #'%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(custom)s'
        self._logfmt = logfmt
        if datefmt is None:
            datefmt = '%Y-%m-%d %H:%M:%S'
        self._datefmt = datefmt
    @property
    def path(self):
         return self._path
    @path.setter
    def path(self, value):
         self._path = value
    @property
    def level(self):
         return self._level
    @level.setter
    def level(self, value):
         self._level = value
    @property
    def logfmt(self):
         return self._logfmt
    @logfmt.setter
    def logfmt(self, value):
         self._logfmt = value
    @property
    def datefmt(self):
         return self._datefmt
    @datefmt.setter
    def datefmt(self, value):
         self._datefmt = value
    def setup(self, logger_name, path=None, logfmt=None, datefmt=None, level=None, streaming=None):
        if path is not None:
            self._path = path
        if logfmt is not None:
            self._logfmt = logfmt
        if datefmt is not None:
            self._datefmt
        if level is not None:
            self._level
#         try:
        logging.basicConfig(
            format = self._logfmt
            , datefmt = self._datefmt
            , filename = self._path
        )
        self._logger = logging.getLogger(logger_name)
        self._logger.setLevel(self._level)
        if streaming is not None:
            ch = logging.StreamHandler()
            ch.setLevel(level)
            ch.setFormatter(
                logging.Formatter(self._logfmt, datefmt=self._datefmt)
            )
            self._logger.addHandler(ch)  
#         except Exception as e:
#             print(e)
    def log_write(self, message, level=logging.DEBUG, extra=None):
        if level is logging.CRITICAL:
            self._logger.critical(message, extra=extra)
        elif level is logging.ERROR:
            self._logger.error(message, extra=extra)
        elif level is logging.WARNING:
            self._logger.warning(message, extra=extra)
        elif level is logging.INFO:
            self._logger.info(message, extra=extra)
        elif level is logging.DEBUG:
            self._logger.debug(message, extra=extra)
    def backup(self, file_path=None, version=True):
        # file_path - novo nome do arquivo de backup
        # version - acresc
        if not version and file_path is None:
            return None
        if file_path is None:
            file_path = self._path
#         try:
        path, file = os.path.split(file_path)
        file_name, extension = os.path.splitext(file)
        if version:
            filename = file_name + f"v{dt.now():%Y.%m.%d.%H.%M}" + extension
        else:
            filename = file_name + extension
#         except Exception as e:
#             print(e)
#         try:
        copy(
            self._path
            , os.path.join(path, filename)
        )
#         except Excetion as e:
#             print(e)
class Zendesk:
    def __init__(self, log_path, login_json, data_fields, log_level=None):
        self.__config(log_path, login_json, data_fields, log_level)
    def __config(self, log_path, login_json, data_fields, log_level):        
        self.data_fields = data_fields
        if log_level is None:
            log_level = logging.INFO
        self.log = LoggingControl(
            log_path
            #, logfmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(parametros)s'
            , level = log_level
        )
        try:
            with open(login_json) as f:
                data_login = json.load(f)
            self.zendesk_user = data_login['user']
            self.zendesk_pwd = data_login['pwd']
            self.__set_sessions()
        except Exception as e:
            print(e)
    def __set_sessions(self):
        credentials = self.zendesk_user, self.zendesk_pwd
        with rq.Session() as self.session:
            self.session.auth = credentials
            return None
    def set_log(self, path=None, level=None, logfmt=None, datefmt=None):
        if path is not None:
            self.log.path = path
        if level is not None:
            self.log.level = level
        if logfmt is not None:
            self.log.logfmt = logfmt
        if datefmt is not None:
            self.log.datefmt = datefmt
    def setup(self, logger_name=None, closed_ids:np.array=None):
        if logger_name is not None:
            logger_name = 'Zendesk'
        self.log.setup(logger_name)
        self.log.log_write('Início do Log', self.log.INFO, {'parametros':None})
        _ = self.__set_max_id()
        self.__create_list_ids(closed_ids)
    def __create_list_ids(self, closed_ids):
        ids = np.array(list(range(1, self.maxid+1)))
        if closed_ids is not None:
            indice = ~np.isin(
                ids
                , closed_ids
            )
            self.ids = ids[indice]
        self.ids = ids 
    def log_backup(self, file_path=None, version=True):
        self.log.backup(file_path, version)
    def __set_max_id(self):
        self.log.log_write('Capturando o maximo id', self.log.DEBUG, {'parametros':None})
        try:
            params = {
                'query': 'type:ticket',
                'sort_by': 'id',
                'sort_order': 'desc'
            }
            url = 'https://paylivrehelp.zendesk.com/api/v2/search.json?' + urlencode(params)
            with self.session.get(url) as response:                
                data = response.json() 
                if len(data['results']) > 0:
                    self.maxid = data['results'][0]['id']
                else:
                    self.maxid = None
                self.log.log_write('Maximo id capturado: '+ str(self.maxid), self.log.DEBUG, {'parametros':str(self.maxid)})
                return self.maxid                
        except Exception as e:
            self.log.log_write(e, self.log.ERROR, {'parametros':None})
    def show_many(self, ids):
        self.log.log_write('show_many::begin', self.log.DEBUG,{'parametros':ids})
        self.log.log_write('Request link:', self.log.DEBUG, {'parametros':'https://paylivrehelp.zendesk.com/api/v2/tickets/show_many.json?'})
        try:
            params = {
                'ids': ids
            }
            url = 'https://paylivrehelp.zendesk.com/api/v2/tickets/show_many.json?' + urlencode(params)
            with self.session.get(url) as response:                
                data = response.json()
                if len(data['tickets']) > 0:
                    self.log.log_write('show_many::end len(data["tickets"]) > 0', self.log.DEBUG,{'parametros':ids})
                    return data
                else:
                    self.log.log_write('show_many::end len(data["tickets"]) <= 0', self.log.DEBUG,{'parametros':ids})
                    return None
        except Exception as e:
            self.log.log_write(e, self.log.ERROR, {'parametros':str(ids)})
    def __check_key_data(self, row_temp, item, keys:list, row_key):
        self.log.log_write('verificada chave'+'-'.join([str(x) for x in keys]), self.log.DEBUG, {'parametros':None})
        self.log.log_write('verificada chave'+'-'.join([str(x) for x in keys]), self.log.DEBUG, {'parametros':json.dumps(item)})
        try:
            item_temp = item
            for key in keys:
                if type(key) is dict and type(item_temp) is list:
                    for i_list in item_temp:
                        if i_list[key['key_name']] == key['key_value']:
                            item_temp = i_list[key['value_name']]
                elif type(key) is not dict:
                    if key in item_temp.keys():
                        item_temp = item_temp[key]
                if type(item_temp) is not dict or (type(item_temp) is not list and type(key) is not dict):
                    if item_temp is None:
                        row_temp[row_key] = np.nan
                    else:
                        row_temp[row_key] = item_temp
                else:
                    row_temp[row_key] = np.nan
            return row_temp
        except Exception as e:
            self.log.log_write(e, self.log.ERROR, {'parametros':str(None)})
    def __data_wrapper(self, data_json):
        table = []
        if data_json is not None:
            self.log.log_write('Request retornou dados', self.log.DEBUG, {'parametros':None})            
            for item in data_json['tickets']:
                row_temp = {}
                for key, values in self.data_fields.items():
                    row_temp = self.__check_key_data(row_temp, item, values, key)
                if type(row_temp['nota_csat']) is list and len(row_temp['nota_csat']) > 0:  
                    for tag in row_temp['nota_csat']:
                        if tag[:len('nr_csat_')] == 'nr_csat_':
                            row_temp['nota_csat'] = int(tag[len('nr_csat_'):])   
                            break
                if type(row_temp['nota_csat']) is list:
                    row_temp['nota_csat'] = np.nan  
                        
                table.append(row_temp)
        return table
    def get_data_ids(self, qtd_requests_minutes=200, qtd_ids_requests=100, break_limit=-1):
        self.log.log_write('get_data_ids::begin', self.log.DEBUG,{'parametros':None})
        self.log.log_write('controlando quantidade de requests', self.log.DEBUG, {'parametros':None})
        start = time.time()
        count_request = 0
        table = []
        for i in range(len(self.ids) // qtd_ids_requests + 1 if len(self.ids) % qtd_ids_requests > 0 else 0):
            print(i,time.time()-start)
            if break_limit>0 and break_limit<=count_request:
                break
            if count_request >= qtd_requests_minutes:
                break_limit -= count_request
                if (60-(time.time()-start)+5) < 0:
                    time.sleep(60)
                else:
                    time.sleep(60-(time.time()-start)+5)
                start = time.time()
                count_request = 0
            table += self.__data_wrapper(
                self.show_many(
                    ', '.join(
                        [str(x) for x in self.ids[i*qtd_ids_requests:(i+1)*qtd_ids_requests]]
                    )
                )
            )
            count_request += 1
        self.log.log_write('get_data_ids::end', self.log.DEBUG,{'parametros':None})
        return table


# In[58]:


#closed_tickets = "SELECT distinct(ticket_id) FROM `paylivre-304215.suporte.closed_tickets` ORDER BY ticket_id ASC"
#query_job = client.query(closed_tickets)

#closed_tickets_ids = []
#for row in query_job:
    #row[0]
    #closed_tickets_ids.append(row[0])


# In[74]:


query={
    'ticket_id':['id'],
    'channel':['via','channel'],
    'created_at':['created_at'],
    'updated_at':['updated_at'],
    'type':['type'],
    'subject':['subject'],
    'raw_subject':['raw_subject'],
    'description':['description'],
    'priority':['priority'],
    'status':['status'],
    'recipient':['recipient'],
    'requester_id':['requester_id'],
    'submitter_id':['submitter_id'],
    'assignee_id':['assignee_id'],
    'group_id':['group_id'],
    'has_incidents':['has_incidents'],
    'is_public':['is_public'],
    'satisfaction_rating':['satisfaction_rating'],
    'dependencia':['fields', {'key_name':'id', 'key_value':1500007589962, 'value_name':'value'}],
    'motivo':['fields', {'key_name':'id', 'key_value':1500003732582, 'value_name':'value'}],
    'is_client':['fields', {'key_name':'id', 'key_value':1500003734322, 'value_name':'value'}],
    'estado_uf':['fields', {'key_name':'id', 'key_value':1500003694961, 'value_name':'value'}],
    'fila':['fields', {'key_name':'id', 'key_value':1500004501442, 'value_name':'value'}],
    'social_message_channel_info':['fields', {'key_name':'id', 'key_value':1500003745142, 'value_name':'value'}],
    'nota_csat':['tags']
}


# In[75]:


a = Zendesk('test.log', 'login_json.json', query, log_level = logging.DEBUG)


# In[76]:


a.setup()


# In[77]:


#tb = a.get_data_ids() 
tb = a.get_data_ids(qtd_requests_minutes=150)


# In[123]:


import pandas as pd
df = pd.DataFrame(tb)
df


# In[130]:


df = df.astype(str)
df['created_at'] = df['created_at'].str.replace('Z', '')
df['updated_at'] = df['updated_at'].str.replace('Z', '')
df['requester_id'] = df['requester_id'].str.replace(' ', '')
df['submitter_id'] = df['submitter_id'].str.replace(' ', '')
df['extraction_date'] = dt.now().strftime("%Y-%m-%d %H:%M:%S")
df['extraction_date'] = df['extraction_date'].str.replace(' ', 'T')
df['created_at'] = df['created_at'].str.replace(' ', 'T')
df['updated_at'] = df['updated_at'].str.replace(' ', 'T')
#df['extraction_date'] = df['extraction_date'].astype(str).str[:10] + 'T' + df['extraction_date'].astype(str).str[10:]
df['extraction_date'] = pd.to_datetime(df['extraction_date'])
df['created_at'] = pd.to_datetime(df['created_at'])
df['updated_at'] = pd.to_datetime(df['updated_at'])
df['group_id'] = df['group_id'].str.split('.').str[0]
df['group_id'] = df['group_id'].str.replace(' ', '')
df['assignee_id'] = df['assignee_id'].str.split('.').str[0]
df['assignee_id'] = df['assignee_id'].str.replace(' ', '')
df = df.replace('nan', 'None', regex=True)
df['ticket_id'] = df['ticket_id'].astype(np.int64)
df['requester_id'] = df['requester_id'].astype(np.int64)
df['submitter_id'] = df['submitter_id'].astype(np.int64)


df.head()
#df.dtypes


# In[131]:


colunas = ['ticket_id','channel','created_at','updated_at','type','subject','raw_subject','description','priority',
          'status','recipient','requester_id','submitter_id','assignee_id','group_id','has_incidents','is_public',
          'satisfaction_rating','motivo','is_client','estado_uf','fila','nota_csat','dependencia','social_message_channel_info', 'extraction_date']


# In[132]:


pd.set_option("display.max_columns", None)
df[colunas]


# In[135]:


job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField(name="ticket_id", field_type="INT64"),
        bigquery.SchemaField(name="channel", field_type="STRING"),
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField(name="created_at", field_type="DATETIME"),
        bigquery.SchemaField(name="updated_at", field_type="DATETIME"),
        bigquery.SchemaField(name="type", field_type="STRING"),
        bigquery.SchemaField(name="subject", field_type="STRING"),
        bigquery.SchemaField(name="raw_subject", field_type="STRING"),
        bigquery.SchemaField(name="description", field_type="STRING"),
        bigquery.SchemaField(name="priority", field_type="STRING"),
        bigquery.SchemaField(name="status", field_type="STRING"),
        bigquery.SchemaField(name="recipient", field_type="STRING"),
        bigquery.SchemaField(name="requester_id", field_type="INT64"),
        bigquery.SchemaField(name="submitter_id", field_type="INT64"),
        bigquery.SchemaField(name="assignee_id", field_type="STRING"),
        bigquery.SchemaField(name="group_id", field_type="STRING"),
        bigquery.SchemaField(name="has_incidents", field_type="STRING"),
        bigquery.SchemaField(name="is_public", field_type="STRING"),
        bigquery.SchemaField(name="satisfaction_rating", field_type="STRING"),
        bigquery.SchemaField(name="motivo", field_type="STRING"),
        bigquery.SchemaField(name="is_client", field_type="STRING"),
        bigquery.SchemaField(name="estado_uf", field_type="STRING"),
        bigquery.SchemaField(name="fila", field_type="STRING"),
        bigquery.SchemaField(name="nota_csat", field_type="STRING"),
        bigquery.SchemaField(name="dependencia", field_type="STRING"),
        bigquery.SchemaField(name="social_message_channel_info", field_type="STRING"),
        bigquery.SchemaField(name="extraction_date", field_type="DATETIME")
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)


# In[136]:


job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)


# In[137]:


job.result()


# In[138]:


append_to_backlog = """INSERT INTO `paylivre-304215.suporte.open_tickets` (ticket_id,channel,created_at,updated_at,type,subject,raw_subject,description,priority,
          status,recipient,requester_id,submitter_id,assignee_id,group_id,has_incidents,is_public,
          satisfaction_rating,motivo,is_client,estado_uf,fila,nota_csat,dependencia,social_message_channel_info,extraction_date)
SELECT * FROM
  `paylivre-304215.suporte.zendesk_tickets`
WHERE status = 'open';
INSERT INTO `paylivre-304215.suporte.new_tickets` (ticket_id,channel,created_at,updated_at,type,subject,raw_subject,description,priority,
          status,recipient,requester_id,submitter_id,assignee_id,group_id,has_incidents,is_public,
          satisfaction_rating,motivo,is_client,estado_uf,fila,nota_csat,dependencia,social_message_channel_info,extraction_date)
SELECT * FROM
  `paylivre-304215.suporte.zendesk_tickets`
WHERE status = 'new';
INSERT INTO `paylivre-304215.suporte.pending_tickets` (ticket_id,channel,created_at,updated_at,type,subject,raw_subject,description,priority,
          status,recipient,requester_id,submitter_id,assignee_id,group_id,has_incidents,is_public,
          satisfaction_rating,motivo,is_client,estado_uf,fila,nota_csat,dependencia,social_message_channel_info,extraction_date)
SELECT * FROM
  `paylivre-304215.suporte.zendesk_tickets`
WHERE status = 'pending';
INSERT INTO `paylivre-304215.suporte.solved_tickets` (ticket_id,channel,created_at,updated_at,type,subject,raw_subject,description,priority,
          status,recipient,requester_id,submitter_id,assignee_id,group_id,has_incidents,is_public,
          satisfaction_rating,motivo,is_client,estado_uf,fila,nota_csat,dependencia,social_message_channel_info,extraction_date)
SELECT * FROM
  `paylivre-304215.suporte.zendesk_tickets`
WHERE status = 'solved';"""

query_job = client.query(append_to_backlog)


# In[ ]:




