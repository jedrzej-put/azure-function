import logging

import azure.functions as func

from . import api_service

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    logging.info('Python api_service function get_data')
    data = api_service.get_data()
    api_service.upload_blob_data(data)
    
    return func.HttpResponse(f"This HTTP triggered function executed successfully.")
