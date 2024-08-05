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

@app.route(route="thread_trigger")
def thread_trigger(req: func.HttpRequest) -> func.HttpResponse:
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

    def upload_answer_to_anaplan(auth_token, model_id, module_id, answer, line_item):
        url = f'https://api.anaplan.com/2/0/models/{model_id}/modules/{module_id}/data'
        headers = {'Authorization': auth_token, 'Accept': 'application/json'}
        body = [{"lineItemId": line_item, "value": answer}]
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
    #refresh_token(auth_token)

    #hardcode model id
    model_id='9815E95A19194E7682BB84D1A920E377'

    #1. Unit price response

    #hardcode the view to retrieve the data
    view_id='395000000000'

    #download the data from anaplan
    contentfromAnaplan=download_data(auth_token, model_id, view_id)
    
    #manipulate the data
    data=StringIO(contentfromAnaplan)
    df=pd.read_csv(data, header=1)
    df=df.rename(columns={"Unnamed: 0":"Location", "Unnamed: 1":"SKU","Unnamed: 2":"Month"})
    df=df.round(1)
    
    #create the file and upload it to the storage+ retrieve the data
    output = StringIO()
    df.to_csv(output, index=False)
    output.seek(0)
    csv_data=output.getvalue().encode('utf-8')
    file1_id=upload_data_to_openai(csv_data)

    #create empty thread
    empty_thread = client.beta.threads.create()
    thread_id=empty_thread.id
    
    aicontroller2_id="asst_ICNbUAWV6zxNrMU2Xdq4k0qs"

    #create a message and append it to the thread
    message = client.beta.threads.messages.create(
        thread_id=thread_id,
        role="user",
        content="""Ti è stato fornito un file .csv con dati sugli Unit Price per SKU. Calcola la varianza *%* dello unit price rispetto al valore di Budget (non badare alla colonna variance).\n
        Individua poi le combinazioni Location_SKU_Month dove la variazione *%* in valore assoluto dello Unit Price è superiore 40%, se presenti elencale in questo formato:\n
        Location_SKU_Month | Var %""",
        attachments=[
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
        time.sleep(1) # Wait for 1 second
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

    upload_answer_to_anaplan(auth_token, model_id, module_id='102000000064', line_item='386000000001', answer=answer)

    #Executive summary answer

    #hardcode parameters to download the data
    view_id='395000000001'

    #download data da anaplan
    ContentfromAnaplan=download_data(auth_token, model_id, view_id)

    #manipulate the data 
    data=StringIO(ContentfromAnaplan)
    df=pd.read_csv(data, header=1)
    df=df.rename(columns={"Unnamed: 0":"Voce CE"})
    df=df.round(1)
    df.drop("Variance", axis=1, inplace=True)
    df.drop([0,1,12,13],axis=0, inplace=True)

    output = StringIO()
    df.to_csv(output, index=False)
    output.seek(0)
    csv_data=output.getvalue().encode('utf-8')
    file2_id=upload_data_to_openai(csv_data)

    empty_thread = client.beta.threads.create()
    
    aicontroller2_id="asst_ICNbUAWV6zxNrMU2Xdq4k0qs"
    thread_id=empty_thread.id

    message = client.beta.threads.messages.create(
        thread_id=thread_id,
        role="user",
        content="""Ti ho fornito dei dati di conto economico nel file in formato .csv.\n
                Calcola le varianze tra actual e budget per ogni voce (ad esclusione del gross margin %)\n
                Commenta in italiano i risultati seguendo queste linee guida:\n
                - utilizza un elenco a punti;\n
                - utilizza al massimo 200 parole;\n
                - non essere verboso;\n
                - evidenzia solamente le voci con gli scostamenti principali partendo dalla voce Net Revenues e concludendo con il commento del Gross Margin""", 
        attachments=[
            {
                "file_id":file2_id,
                "tools":[{"type":"code_interpreter"}]
             }
        ]
    )

    run = client.beta.threads.runs.create(
        thread_id=thread_id,
        assistant_id=aicontroller2_id,
    )

    while run.status in ['queued', 'in_progress', 'cancelling']:
        time.sleep(1) # Wait for 1 second
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

    upload_answer_to_anaplan(auth_token, model_id, module_id='102000000064', line_item='386000000004', answer=answer)


    #Variance Analysis Answer

    #retrieve question from anaplan
    view_id='387000000002' 
    questionfromAnaplan=download_data(auth_token, model_id, view_id)
    question=questionfromAnaplan.replace('Question 2,', '').strip()
    question=question.replace('\r\n', '\n').strip()
    #print(question)

    #download the data
    view_id='399000000001'

    #download variance analysis data
    ContentFromAnaplan=download_data(auth_token, model_id, view_id)

    #manipulate the data 
    data=StringIO(ContentFromAnaplan)
    df=pd.read_csv(data)
    df=df.rename(columns={"Unnamed: 0":"Location", "Unnamed: 1":"SKU","Unnamed: 2":"Month"})
    df=df.round(1)

    #upload data to openai storage
    output = StringIO()
    df.to_csv(output, index=False)
    output.seek(0)
    csv_data=output.getvalue().encode('utf-8')
    file3_id=upload_data_to_openai(csv_data)

    #create empty thread
    empty_thread = client.beta.threads.create()
    thread_id=empty_thread.id

    #===hardcode ourd ids===
    aicontroller3_id="asst_a7rxfFsov34qEsIXiGMxCHtO"

    #create a message and append it to the thread
    message = client.beta.threads.messages.create(
        thread_id=thread_id,
        role="user",
        content=question, 
        attachment=[
            {
            'file_id':file3_id,
            'tools':[{'type':'code_interpreter'}]
            }
        ]
    )

    #create run
    run = client.beta.threads.runs.create(
        thread_id=thread_id,
        assistant_id=aicontroller3_id,
    )

    while run.status in ['queued', 'in_progress', 'cancelling']:
        time.sleep(1) # Wait for 1 second
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

    messages = client.beta.threads.messages.list(thread_id)

    last_message = messages.data[0]
    answer = last_message.content[0].text.value

    upload_answer_to_anaplan(auth_token, model_id, module_id='102000000064', line_item='386000000002', answer=answer)

    return func.HttpResponse(
            "This HTTP triggered function executed successfully.",
            status_code=200
    )