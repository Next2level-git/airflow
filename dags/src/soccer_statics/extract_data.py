from airflow.models import Variable
import logging
import requests
import pandas as pd
import json
import ast


class Request_Data:

    def __init__(self):
        self.url = Variable.get("URL_SOCCER_STATICS")
        self.header = json.loads(Variable.get("PASSWORD_HEADERS_API_SOCCER"))
        log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        self.__logger = logging.getLogger(__name__)

    def request_return(self):

        data = pd.DataFrame([])
        leagues = ast.literal_eval(Variable.get("LEAGUES_TO_CALL"))
        year = Variable.get("YEAR_TO_CALL")
        for id_league in leagues:
            query_api = {
                "league": f"{id_league}",
                "season": f"{year}"
            }
            print(query_api)
            response = requests.get(self.url, headers=self.header, params=query_api)
            if response.status_code == 200:
                data_new = response.json()
                data_new = [
                    item
                    for item in data_new["response"]
                    if item["fixture"]["status"]["short"] == "FT"
                ]
                data_new = pd.DataFrame(data_new)
                data = pd.concat([data_new, data], ignore_index=True)

        return data
