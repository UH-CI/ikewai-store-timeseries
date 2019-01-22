#!/usr/bin/env python
# coding: utf-8


import sys, getopt
import os.path
import fileinput
import requests
import json
import urllib
import  urllib.parse
import pandas as pd
import getpass
from requests.auth import HTTPBasicAuth

#***************AGAVE API FUNCTIONS******************************
def getTokenIkeWai(username):
    res = requests.get('https://'+endpoint+':8000', auth=HTTPBasicAuth(username, getpass.getpass()))
    return res

def listMetadata(token, query="", limit=10, offset=0):
    safe_query = urllib.parse.quote(query.encode('utf8'))
    headers = {
        'authorization': "Bearer " + token,
        'content-type': "application/json",
    }
    res = requests.get('https://'+endpoint+'/meta/v2/data?q='+safe_query+'&limit='+str(limit)+'&offset='+str(offset), headers=headers,verify=False)
    resp = json.loads(res.content)
    return resp['result']

def getMetadata(token, uuid):
    headers = {
        'authorization': "Bearer " + token,
        'content-type': "application/json",
    }
    res = requests.get('https://'+endpoint+'/meta/v2/data/'+uuid, headers=headers,verify=False)
    #print(res.content)
    resp = json.loads(res.content)
    return resp['result']

def updateMetadata(token, uuid, data):
    headers = {
        'authorization': "Bearer " + token,
        'content-type': "application/json",
    }
    res = requests.post('https://'+endpoint+'/meta/v2/data/'+uuid, json=data, headers=headers,verify=False)
    resp = json.loads(res.content)
    return resp

def createMetadata(token, data):
    headers = {
        'authorization': "Bearer " + token,
        'content-type': "application/json",
    }
    res = requests.post('https://'+endpoint+'/meta/v2/data/', json=data, headers=headers,verify=False)
    print(res.content)
    resp = json.loads(res.content)
    return resp['result']

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

def getMetadataPems(token,uuid):
    headers = {
        'authorization': "Bearer " + token,
        'content-type': "application/json",
    }
    res = requests.get('https://'+endpoint+'/meta/v2/data/'+uuid+'/pems/', headers=headers,verify=False)
    resp = json.loads(res.content)
    return resp


#********************

def parseTimeseries(token,uuid,filepath):
    variables={};
    sites={};
    this_df = pd.read_csv(filepath);
    #this should fetch a Timeseries parsing template
    timeseries = getMetadata(token, uuid);
    #change timeseries dataframe column header to variablename+units
    var_list = [];
    for myvar in timeseries['value']['columns']:
      var_list.append(myvar['variable_id']);
      print(myvar['variable_id']+":"+str(myvar['column_number']));
      variables[myvar['variable_id']] = getMetadata(token,myvar['variable_id']);
      print(variables[myvar['variable_id']]['value']['variable_name']);
      this_df.rename(columns={this_df.columns[myvar['column_number']-1]:variables[myvar['variable_id']]['value']['variable_name']+':'+variables[myvar['variable_id']]['value']['unit']}, inplace=True)
    this_data = json.loads(this_df.to_json(orient='records'));
    #create our timeseries instance object
    this_timeseries = {'name':'Timeseries'};
    this_timeseries['associationIds']= var_list;
    this_timeseries['value'] = timeseries['value'];
    this_timeseries['value']['variables'] =variables;
    this_timeseries['value']['filename'] = filepath;
    this_timeseries['permissions'] = [{"username":"jgeis","permission":"ALL"},{"username":"ikewai-admin","permission":"ALL"},{"username":"ikeadmin","permission":"ALL"},{"username":"public","permission":"READ"}]
    new_timeseries = createMetadata(token,this_timeseries);
    site_list = [];
    for row in this_data:
        this_obs = {'name':'Observation'};
        print(row);
        if row['site-id'] in sites:
            #do nothing
            print("already exists");
        else:
            sites[row['site-id']] = listMetadata(token, "{'name':'Site','value.id':'"+row['site-id']+"'}", 1, 0)[0]
            site_list.append(sites[row['site-id']]['uuid'])
            print(sites[row['site-id']]);
        #create Observation record
        assc_list = []
        assc_list = var_list;
        assc_list.append(sites[row['site-id']]['uuid']);
        assc_list.append(new_timeseries['uuid']);
        assc_list = list(set(assc_list))
        this_obs['associationIds']= assc_list;
        #this_obs['associationIds']= var_list;
        #this_obs['associationIds'].append(sites[row['site-id']]['uuid']);
        #this_obs['associationIds'].append(new_timeseries['uuid']);
        #this_obs['associationIds'].append(timeseries['uuid']);
        this_obs['value'] = row;
        this_obs['permissions'] = [{"username":"jgeis","permission":"ALL"},{"username":"ikewai-admin","permission":"ALL"},{"username":"ikeadmin","permission":"ALL"},{"username":"public","permission":"READ"}]
        print(this_obs);
        print(createMetadata(token,this_obs));
    new_ts_assoc = []
    new_ts_assoc = new_timeseries['associationIds']
    print("********************")
    print(new_ts_assoc)
    new_ts_assoc = new_ts_assoc + site_list;
    new_ts_assoc = new_ts_assoc + site_list;
    print(new_ts_assoc)
    new_timeseries['associationIds'] = list(set(new_ts_assoc))
    print(updateMetadata(token,new_timeseries['uuid'],new_timeseries));
    #update site with variable associations

def main(argv):
    global endpoint
    endpoint=""
    token=""
    inputfile =""
    uuid = ""
    try:
        opts, args = getopt.getopt(argv,"he:t:i:u",["endpoint=","token=","inputfile=","uuid="])
    except getopt.GetoptError:
        print('TRY parse_spreadsheet_timeseries.py -e <Agave endpoint> -t <Valid Agave Auth Token> -i <inputfile CSV> -u <Timeseries Metadata UUID>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('parse_spreadsheet_timeseries.py -e <Agave endpoint> -t <Valid Agave Auth Token> -i <inputfile CSV> -u <Timeseries Metadata UUID>')
            sys.exit()
        elif opt in ("-e", "--endpiont"):
            endpoint= arg
        elif opt in ("-t", "--token"):
            token= arg
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-u", "--uuid"):
            uuid = arg
        else:
            assert False, "unhandled option"
    print(endpoint)
    print(token)
    print(inputfile)
    print(uuid)
    parseTimeseries(token,uuid,inputfile)

if __name__ == "__main__":
   main(sys.argv[1:])
