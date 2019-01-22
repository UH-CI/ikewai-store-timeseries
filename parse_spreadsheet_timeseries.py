#!/usr/bin/env python
# coding: utf-8

# In[35]:


import requests
import json
import urllib
import  urllib.parse
import pandas as pd
import getpass
from requests.auth import HTTPBasicAuth
endpoint = 'agaveauth.its.hawaii.edu'
token = 'eb4b2d378355734dfa71d4e95cb985'


# In[36]:


def getTokenIkeWai(username):
    res = requests.get('https://'+endpoint+':8000', auth=HTTPBasicAuth(username, getpass.getpass()))
    return res


# In[37]:


def listMetadata(token, query="", limit=10, offset=0):
    safe_query = urllib.parse.quote(query.encode('utf8'))
    headers = {
        'authorization': "Bearer " + token,
        'content-type': "application/json",
    }
    res = requests.get('https://'+endpoint+'/meta/v2/data?q='+safe_query+'&limit='+str(limit)+'&offset='+str(offset), headers=headers,verify=False)
    resp = json.loads(res.content)
    return resp['result']


# In[38]:


def getMetadata(token, uuid):
    headers = {
        'authorization': "Bearer " + token,
        'content-type': "application/json",
    }
    res = requests.get('https://'+endpoint+'/meta/v2/data/'+uuid, headers=headers,verify=False)
    resp = json.loads(res.content)
    return resp['result']


# In[39]:


def updateMetadata(token, uuid, data):
    headers = {
        'authorization': "Bearer " + token,
        'content-type': "application/json",
    }
    res = requests.post('https://'+endpoint+'/meta/v2/data/'+uuid, json=data, headers=headers,verify=False)
    resp = json.loads(res.content)
    return resp


# In[128]:


def createMetadata(token, data):
    headers = {
        'authorization': "Bearer " + token,
        'content-type': "application/json",
    }
    res = requests.post('https://'+endpoint+'/meta/v2/data/', json=data, headers=headers,verify=False)
    resp = json.loads(res.content)
    return resp


# In[40]:


#permission needs to be READ READ-WRITE or ALL
def updateMetadataPem(token, uuid, username, permission):
    headers = {
        'authorization': "Bearer " + token,
        'content-type': "application/json",
    }
    data = {'permission':permission}
    res = requests.post('https://'+endpoint+'/meta/v2/data/'+uuid+'/pems/'+username, json=data, headers=headers,verify=False)
    resp = json.loads(res.content)
    return resp
    


# In[41]:


def getMetadataPems(token,uuid):
    headers = {
        'authorization': "Bearer " + token,
        'content-type': "application/json",
    }
    res = requests.get('https://'+endpoint+'/meta/v2/data/'+uuid+'/pems/', headers=headers,verify=False)
    resp = json.loads(res.content)
    return resp


# In[158]:


def parseTimeseries(token,uuid,filepath):
    variables={};
    sites={};
    this_df = pd.read_csv(filepath);
    timeseries = getMetadata(token, uuid);
    #change timeseries dataframe column header to variablename+units
    var_list = []
    for myvar in timeseries['value']['columns']:
      var_list.append(myvar['variable_id'])
      print(myvar['variable_id']) 
      print(myvar['column_number'])
      variables[myvar['variable_id']] = getMetadata(token,myvar['variable_id'])
      print(variables[myvar['variable_id']]['value']['variable_name'])
      this_df.rename(columns={this_df.columns[myvar['column_number']-1]:variables[myvar['variable_id']]['value']['variable_name']+':'+variables[myvar['variable_id']]['value']['unit']}, inplace=True)
    this_data = json.loads(this_df.to_json(orient='records'))
    for row in this_data:
        this_obs = {'name':'Observation'}
        print(row)
        if row['site-id'] in sites:
            #do nothing
            print("already exists")
        else:
            sites[row['site-id']] = listMetadata(token, "{'name':'Site','value.id':'"+row['site-id']+"'}", 1, 0)[0]
            print(sites[row['site-id']])
        #create Observation record
        this_obs['associationIds']= var_list;
        this_obs['associationIds'].append(sites[row['site-id']]['uuid'])
        this_obs['value'] = row;
        print(this_obs)
        #print(createMetadata(token,this_obs))
    #update site with variable associations
        


# In[152]:


meta[0]


# In[ ]:


vmyar = getMetadata(token,'2748921731548844521-242ac1111-0001-012')


# In[ ]:


vmyar


# In[ ]:


tempvar = vmyar
tempvar['value']['name']="varsOne"


# In[ ]:


tempvar['value'] = vmyar['value']
tempvar['value']['name']="vars1"
tempvar['associationIds'] = vmyar['associationIds']
tempvar


# In[ ]:


upvar = updateMetadata(token,'2748921731548844521-242ac1111-0001-012',tempvar)


# In[ ]:


upvar['result']['value']


# In[ ]:


pems = getMetadataPems(token,'2748921731548844521-242ac1111-0001-012')


# In[ ]:


pems


# In[ ]:


uppems = updateMetadataPem(token,'2748921731548844521-242ac1111-0001-012','public','READ')


# In[ ]:


uppems


# In[1]:


import pandas as pd


# In[154]:


df = pd.read_csv('ikewai-spreadtest.csv')


# In[7]:


js = df.to_json(orient='records')


# In[8]:


js


# In[12]:


myjs = json.loads(js)


# In[13]:


myjs[0]


# In[21]:


ts = getMetadata(token,'9051709891106968041-242ac1111-0001-012')


# In[22]:


ts


# In[55]:


df.columns[0]


# In[ ]:


{
  'name':'Observation',
  'associationIds':['7419369018632835561-242ac1111-0001-012','']
  'value':{'time':'datetime','Temperature-degree(C)':'1','Chloride-percent(%)':'2'}
}


# In[ ]:


parse timeseries metadata - fetch variable metadata objects
replace column header with variableName + unit combo
convert dataframe to json orienting with 'record'
iterate through json array - fetch site metadata by ID -store in a site hash indexed by site ID(if exists don't fetch)
build Observation metadata obj associating variables, site and timseries, set permissions and create metadata
update timeseries with site associations


# In[27]:





# In[28]:





# In[33]:





# In[48]:


ts = getMetadata(token,'9051709891106968041-242ac1111-0001-012')


# In[60]:


len(ts['value']['columns'])
variables = {}
for myvar in ts['value']['columns']:
  print(myvar['variable_id'])
  print(myvar['column_number'])
  variables[myvar['variable_id']] = getMetadata(token,myvar['variable_id'])
  print(variables[myvar['variable_id']]['value']['variable_name'])
  df.rename(columns={df.columns[myvar['column_number']-1]:variables[myvar['variable_id']]['value']['variable_name']+':'+variables[myvar['variable_id']]['value']['unit']}, inplace=True)
df


# In[75]:


df.rename(columns={df.columns[2]:"somethingelse"},inplace=True)
df.rename(columns={df.columns[3]:"anothersomething"},inplace=True)


# In[86]:


mine =  listMetadata(token, "{'name':'Site','value.id':'its-fountain1'}", 1, 0)


# In[89]:


mine[0]['uuid']


# In[103]:


obs = []
obs= variables.keys()


# In[150]:


myvar


# In[155]:


df.rename(columns={df.columns[myvar['column_number']-1]:variables[myvar['variable_id']]['value']['variable_name']+':'+variables[myvar['variable_id']]['value']['unit']}, inplace=True)
df


# In[ ]:





# In[159]:


parseTimeseries(token,'9051709891106968041-242ac1111-0001-012','ikewai-spreadtest.csv')


# In[ ]:




