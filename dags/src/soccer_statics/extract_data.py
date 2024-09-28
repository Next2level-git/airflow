from airflow.models import Variable
import logging
import requests
import pandas as pd

class Request_Data:

    def __init__(self):
        self.url = Variable.get("URL_SOCCER_STATICS")
        self.query_api = Variable.get("QUERIE_API_SOCCER")
        self.header = Variable.get("PASSWORD_HEADERS_API_SOCCER")
        log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        self.__logger = logging.getLogger(__name__)


    def request_return(self):

        data = pd.Dataframe([])
        response = requests.get(self.url, headers=self.header, params=self.query_api)
        if response.status_code == 200:
            data = response.json()
            data = [item for item in data['response'] if item['fixture']['status']['short'] == 'FT']
            data = pd.Dataframe(data)

        return data


