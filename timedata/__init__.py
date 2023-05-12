import logging
import os
import datetime

import requests
# import io
import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# import http.client
# import ssl
# import json
import pandas as pd
import string
import pathlib

import datetime



def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function ran at %s', datetime.datetime.utcnow().isoformat())

    certs_folder_path = os.path.join(os.getcwd(), "CERTS")

    certFile = os.path.join(certs_folder_path, "Pantar_Azure_AD.pem")
    keyFile = os.path.join(certs_folder_path, "Pantar_Azure_AD.key")

    today = datetime.datetime.today().date().strftime('%Y-%m-%d')
    # certFile = "..\CERTS\Pantar_Azure_AD.pem"
    # keyFile =  "..\CERTS\Pantar_Azure_AD.keyy"

    print(certFile,keyFile)
    host = 'accounts.eu.adp.com'

    request_url='https://api.eu.adp.com/auth/oauth/v2/token'
    # https://api.eu.adp.com/auth/oauth/v2/token


    #client_id = "1b386e04-fbf4-48b5-843b-1b76a209d472" #this is the testing one client_id to testing env

    client_id = os.environ["client_id"]  # this is the working one
    client_secret = os.environ["client_secret"]
    
    data_ = []



    request_body_dict={  'client_id':client_id  ,
            'client_secret': client_secret,
            'grant_type': 'client_credentials' }


    # request_headers = {
    #     'Content-Type': 'application/json',
    
    # }


    rr = requests.post(request_url, cert=(certFile, keyFile), data = request_body_dict)
    bearerToken = rr.json()['access_token']
    print("Bearer token: ", rr.json()['access_token'])
    headers_ = { "Authorization":  "Bearer " + bearerToken}
    

    data_ = []







#     url_ = "https://api.eu.adp.com/hr/v2/workers"
#url_ = "https://api.eu.adp.com/hr/v2/worker-demographics?top=3"

    for i in range(int(os.environ['max'])):



        a= i*100
        
        print(a)

        b = str(a)

    #     url_ = "https://api.eu.adp.com/hr/v2/worker-demographics?$top=100&$skip={}".format(b)
        
        url_1 = "https://api.eu.adp.com/hr/v2/worker-demographics?$top=100&$skip={}".format(b) 
        url_2 = "&$filter=/workers/workAssignments/actualStartDate le '{}'".format(today)
        url_3 = " and /workers/workAssignments/terminationDate gt '{}'".format(today)
        url_ = url_1+url_2+url_3
        
        print(url_)
        #url_ = "https://api.eu.adp.com/hr/v2/worker-demographics"

        #url_ = "https://api.eu.adp.com/hr/v2/workers?top=3"

        try: 
            rrrr=requests.get(url_, cert=(certFile, keyFile), headers=headers_,timeout = (500, 500))
    #             print(rrrr.text)

            result = rrrr.json()

    #             if len(result)['workers']==0:
    #                 break

            if len(result['workers']) == 0:
                break
                


            for i in result['workers']:

    #                 print(i['workerID']['idValue'],i['businessCommunication']['emails'][0]['emailUri'],i['workAssignments'][0]['homeOrganizationalUnits'][0]['nameCode']['codeValue'],)


                try: 
                    workerID = i['workerID']['idValue']
                except:
                    workerID = None


    #             try:
    #                 email = i['businessCommunication']['emails'][0]['emailUri']

    #             except:
    #                 email = ""

                try:

                    homeOrganizationalUnits = i['workAssignments'][0]['homeOrganizationalUnits'][0]['nameCode']['codeValue']


                except:

                    homeOrganizationalUnits = ""
                    
                try: 
                    homeOrganizationalUnitsLongName = i['workAssignments'][0]['homeOrganizationalUnits'][0]['nameCode']['longName']
                    
                except:
                    
                    homeOrganizationalUnitsLongName = ""
                    
                    
                    

                try:
                    jobCode= i['workAssignments'][0]['jobCode']['codeValue']

                except:
                    jobCode = ""



                data_.append({'workerID':workerID,'homeOrganizationalUnits': homeOrganizationalUnits, 'jobCode':jobCode ,'homeOrganizationalUnitsLongName':homeOrganizationalUnitsLongName})






        except:

            print("TimeOUT!!!!")
                


    df = pd.DataFrame(data_)

    df1 = df.rename(columns={"workerID": "EmployeeID", "homeOrganizationalUnits": "extensionAttribute2" , "homeOrganizationalUnitsLongName": "Department","jobCode":"extensionAttribute1"})


    # url = 'https://example.com/data'
    # response = requests.get(url)
    
    # # Parse the response data as a DataFrame
    # df = pd.read_csv(io.StringIO(response.text))
    
    # Connect to the Azure Storage account
    connection_string = os.environ['AzureWebJobsStorage']
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # Define the name of the container and blob
    container_name = 'pantar-data'
    blob_name = 'mydata.csv'
    
    # Create a ContainerClient object
    container_client = blob_service_client.get_container_client(container_name)
    
    # Create a BlobClient object
    blob_client = container_client.get_blob_client(blob_name)
    
    # Convert the DataFrame to a CSV file and upload it to Azure Storage
    csv_data = df1.to_csv(index=False)
    blob_client.upload_blob(csv_data, overwrite=True)
