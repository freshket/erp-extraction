import requests
import pandas as pd
from datetime import datetime,timedelta
from google.cloud import bigquery
import pandas as pd
from google.oauth2 import service_account
import http.client
import mimetypes
from codecs import encode
import json

def get_token():

    conn = http.client.HTTPSConnection("login.microsoftonline.com")
    dataList = []
    boundary = 'wL36Yn8afVp8Ag7AmP8qZ0SA4n1v9T'
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=client_id;'))

    dataList.append(encode('Content-Type: {}'.format('text/plain')))
    dataList.append(encode(''))

    dataList.append(encode("7b487f75-ae18-4a71-bb86-d1ac4cdb4588"))
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=client_secret;'))

    dataList.append(encode('Content-Type: {}'.format('text/plain')))
    dataList.append(encode(''))

    dataList.append(encode("Vpl8Q~orPiugl36kBHuDUAEaaLW81oJtJr~SGcOD"))
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=grant_type;'))

    dataList.append(encode('Content-Type: {}'.format('text/plain')))
    dataList.append(encode(''))

    dataList.append(encode("client_credentials"))
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=scope;'))

    dataList.append(encode('Content-Type: {}'.format('text/plain')))
    dataList.append(encode(''))

    dataList.append(encode("https://api.businesscentral.dynamics.com/.default"))
    dataList.append(encode('--'+boundary+'--'))
    dataList.append(encode(''))
    body = b'\r\n'.join(dataList)
    payload = body
    headers = {
    'Cookie': 'fpc=AoZccsUIp5xMl2xPla1g5Qgoaz2EAQAAAJCcKd0OAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd',
    'Content-type': 'multipart/form-data; boundary={}'.format(boundary)
    }
    conn.request("GET", "/f5030c5d-b648-4145-80d3-72361127bbb2/oauth2/v2.0/token?resource=https://api.businesscentral.dynamics.com/", payload, headers)
    res = conn.getresponse()
    data = res.read()
    json_token = json.loads(data.decode("utf-8"))
    return json_token['token_type'] + ' ' + json_token['access_token']


def upload_data(df,table,dataset_id):
    ## set up location
    print('data destination processing')
    # Construct a BigQuery client object.
    client = bigquery.Client()
    project_id = client.project
    table_id=f"{project_id}.{dataset_id}.{table}"
    print("THE BIGQUERY LOCATION NAME OF TABLE IS", table_id)
    print("got BQ client object")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        df, 
        table_id, 
        job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.
    client.get_table(table_id)  # Make an API request.
    print(f"Loaded {table} to {table_id}")

def processing_data(df):
    ## delete unnecessary column
    if '@odata.etag' in df.columns:
        del df['@odata.etag']
    df = df.rename(str.lower, axis = 'columns')
    df = df.rename(columns = { 'no' : 'no_'})
    print('rename columns completed')
    # df = df.replace('',None, inplace =True)
    # print('return empty string to null completed')
    return df


def get_api_data(api_url):
    all_data = []
    token = get_token()
    payload = {}
    headers = {'Authorization': token }
    print(f'starting extract data {api_url}')
    while api_url:
        response = requests.request('GET', api_url, headers=headers, data=payload)
        data = response.json()
        df = pd.DataFrame(data['value'])
        # Extract and append the data from the current page
        print('starting extracting the next page')
        all_data.append(df)
        # Check if there is a nextLink in the response
        if '@odata.nextLink' in data:
            api_url = data['@odata.nextLink']
            print(f"get the next page url: {api_url}")
        else:
            print('no more page')
            # No more pages, break out of the loop
            break
    print('downloaded all data to the dataframe')
    erp_df = pd.concat(all_data,ignore_index=True)
    erp_df['snapshot_date'] = datetime.now() 
    print('get data completed')
    return erp_df


def get_api_header_data_n_days_ago(base_url,table_name,col,n,destination_dataset,type_ = None ):
    today = datetime.today()
    ## get date list
    date = today - timedelta(days = n)
    date = date.strftime('%Y-%m-%d')
    print(f'start loading data at {date}')
    api_url = base_url
    if type_ == 'inv':
        api_url = base_url + f"?$filter={col} eq {date}"
    elif type_ == 'cn':
        api_url = base_url + f"?$filter={col} ge {date}"
    erp_df = get_api_data(api_url) 
    # all_data.append(data)  
    # erp_df = pd.concat(all_data)
    erp_df = processing_data(erp_df)
    upload_data(erp_df,table=table_name,dataset_id=destination_dataset)
    
def get_api_line_data(base_url,header_name,table_name,destination_dataset):
    client = bigquery.Client()
    sql = f"""
        select distinct no_
        from `freshket-dev.{destination_dataset}.{header_name}`
"""
    df = client.query(sql).to_dataframe()
    all_data = []
    ## get date list
    for doc_no in df['no_']:
        api_url = base_url + f"?$filter=Document_No eq '{doc_no}'"
        data = get_api_data(api_url) 
        all_data.append(data) 
        c = len(all_data)
        print(f'now we got {c} documents')
    erp_df = pd.concat(all_data)
    erp_df = processing_data(erp_df)
    upload_data(erp_df,table=table_name,project_id=client.project,dataset_id=destination_dataset)

def vendor(url,table_name,destination_dataset = 'dynamic_bc'):
    df = get_api_data(url)
    df = processing_data(df)
    upload_data(df,table=table_name,dataset_id=destination_dataset)

