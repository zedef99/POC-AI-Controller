import azure.functions as func
import logging
from datetime import datetime
import base64
import requests
import json
import pandas as pd
from io import StringIO
from openai import OpenAI
import os
import time

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="http_trigger_qafree")
def http_trigger_qafree(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    def authenticate(creds):
        cred_string = 'Basic ' + base64.b64encode(creds.encode('utf-8')).decode('utf-8')
        headers = {'Authorization': cred_string}
        token_url = 'https://auth.anaplan.com/token/authenticate'
        response = requests.post(token_url, headers=headers)
        token_json = response.json()
        auth_token = 'AnaplanAuthToken ' + token_json['tokenInfo']['tokenValue']
        print(auth_token)
        return auth_token

    def download_data(auth_token, model_id, view_id):
        tran_url = f'https://api.anaplan.com/2/0/models/{model_id}/views/{view_id}/data'
        header = {'Authorization' : auth_token, 'Accept' : 'text/csv'}
        response = requests.get(tran_url, headers = header)
        return response.text

    def upload_answer_to_anaplan(auth_token, model_id, module_id, answer, line_item_name, dimension_name, item_name):
        url = f'https://api.anaplan.com/2/0/models/{model_id}/modules/{module_id}/data'
        headers = {'Authorization': auth_token, 'Accept': 'application/json'}
        body = [{"lineItemName": line_item_name, "dimensions":[{"dimensionName":dimension_name, "itemName":item_name}], "value":answer}]
        response = requests.post(url, headers=headers, json=body)
        f=json.loads(response.text)
        print(f)

    def upload_data_to_openai(content,purpose="assistants"):
        try:
            file_response = client.files.create(file=content, purpose=purpose)
            return file_response.id
        except Exception as e:
            print(f"Failed to upload file to OpenAI: {e}")
            return None
        
    def delete_all_openai_files():
        try:
            files = client.files.list()
            if files.data:
                for file in files.data:
                    client.files.delete(file.id)
                print("Tutti i file sono stati eliminati con successo.")
            else:
                print("Nessun file da eliminare.")
        except Exception as e:
            print(f"Errore durante l'eliminazione dei file: {e}")


    #Activate Openai Client
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        raise EnvironmentError("OPENAI_API_KEY environment variable not set")
    client=OpenAI(api_key=openai_api_key, default_headers={"OpenAI-Beta":"assistants=v2"})

    #delete all the files
    delete_all_openai_files()

    #token creation for anaplan
    creds = os.getenv("CREDS")  
    auth_token = authenticate(creds)

    #hardcode model id
    model_id='9815E95A19194E7682BB84D1A920E377'

    #retrieve question from anaplan
    view_id='411000000002' #to retrieve the question
    questionfromAnaplan=download_data(auth_token, model_id, view_id)
    question=questionfromAnaplan.replace('1\r\nQuestion,', '').strip()

    #hardcode the view to retrieve the data
    view_id='395000000003'

    #download the data from anaplan
    contentfromAnaplan=download_data(auth_token, model_id, view_id)

    #manipulate the data
    data=StringIO(contentfromAnaplan)
    df=pd.read_csv(data, header=1)
    df=df.rename(columns={"Unnamed: 0":"Location", "Unnamed: 1":"SKU","Unnamed: 2":"VoceCE"})
    df=df.round(2)

    #create the file and upload it to the storage + retrieve the data
    output = StringIO()
    df.to_csv(output, index=False)
    output.seek(0)

    csv_data=output.getvalue().encode('utf-8')

    #upload file to openai
    file1_id=upload_data_to_openai(csv_data)

    #create empty thread
    empty_thread = client.beta.threads.create()
    thread_id=empty_thread.id

    aicontroller2_id="asst_GdkDhLGVcvt6MUyFHuXwXXJf"

    message = client.beta.threads.messages.create(
        thread_id=thread_id,
        role="user",
        content=question,
        attachments=
        [
            {
                "file_id":file1_id,
                "tools":[{"type":"code_interpreter"}]
            }
        ]
    )

    #create run
    run = client.beta.threads.runs.create(
    thread_id=thread_id,
    assistant_id=aicontroller2_id
    )

    while run.status in ['queued', 'in_progress', 'cancelling']:
        time.sleep(1) 
        run = client.beta.threads.runs.retrieve(
            thread_id=thread_id,
            run_id=run.id
        )

        if run.status == 'completed': 
            messages = client.beta.threads.messages.list(
                thread_id=thread_id
            )
            print(messages)
        else:
            print(run.status)

    messages = client.beta.threads.messages.list(thread_id=thread_id)
    last_message = messages.data[0]
    answer = last_message.content[0].text.value
    print(f"Assistant Response: {answer}")

    upload_answer_to_anaplan(auth_token=auth_token, model_id=model_id, module_id="102000000076", answer=answer, line_item_name="Answer", dimension_name="dummy", item_name="1")

    
    return func.HttpResponse(
            "This HTTP triggered function executed successfully.",
            status_code=200
    )