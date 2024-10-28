import pandas as pd
import requests
from next_connection.data_connection import PostgreSQLConnection
import logging
from airflow.models import Variable
import src.social_media.queries as queries
import src.social_media.utils as utils
import json
import ast
from datetime import datetime


class Daily_Instagram:

    def __init__(self):
        self.host_db = Variable.get("HOST_DB_NEXT")
        self.user_db = Variable.get("USER_DB_NEXT")
        self.password_db = Variable.get("PASSWORD_DB_NEXT")
        self.db = Variable.get("DB_NEXT")
        self.url = Variable.get("URL_SOCIAL_MEDIA_INSTAGRAM")
        self.header = json.loads(Variable.get("PASSWORD_HEADERS_API_INSTAGRAM"))
        log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        self.__logger = logging.getLogger(__name__)

    def run(self):

        nxdb = PostgreSQLConnection(
            user=self.user_db,
            password=self.password_db,
            host_name=self.host_db,
            database=self.db,
        )
        nxdb.create_connection()
        self.__logger.info("Start extract loaded data")
        new_data = self.__transform_data()
        if not new_data.empty:
            self.__logger.info("Start load new data")
            self.__load(
                conn=nxdb,
                data=new_data,
                schema="social_media",
                dest_table="user_follow_stats",
                truncate=False,
            )
        nxdb.destroy_connection()

    def __extract_api_information(self):
        accounts = ast.literal_eval(Variable.get("ACCOUNTS_SOCIAL_MEDIA"))
        load_data = pd.DataFrame([])
        for account in accounts:
            query_api = {"username": f"{account}"}
            response = requests.get(self.url, headers=self.header, params=query_api)
            if response.status_code == 200:
                response_data = response.json()
                df = pd.DataFrame(response_data)
                df_new = df.transpose()
                df_new = df_new.drop(index="status")
                df_new = df_new.reset_index()
                df_new = df_new[
                    [
                        "id",
                        "follower_count",
                        "following_count",
                        "media_count",
                    ]
                ]
                load_data = pd.concat([df_new, load_data], ignore_index=True)
        return load_data

    def __transform_data(self):
        result_append = self.__extract_api_information()
        result_append = result_append.rename(
            columns={
                "id": "user_id",
                "follower_count": "followers",
                "following_count": "following",
                "media_count": "publications",
            }
        )
        if not result_append.empty:
            result_append["following"] = result_append["following"].astype(
                "Int64"
            )
            result_append["followers"] = result_append["followers"].astype(
                "Int64"
            )
            result_append["publications"] = result_append["publications"].astype(
                "Int64"
            )
            result_append["date"] = pd.to_datetime(
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            )
            result_append = result_append[
                [
                    "user_id",
                    "date",
                    "followers",
                    "following",
                    "publications",
                ]
            ]

        return result_append

    def __load(self, conn, data, dest_table: str, schema="public", truncate=True):
        try:
            self.__logger.info(f"writing {dest_table} table")
            conn.execute_insert(
                data_frame=data, schema=schema, table_name=dest_table, truncate=truncate
            )
            self.__logger.info(f"{len(data)} rows load in {dest_table}")
            self.__logger.info(
                f"loading operation finished successfully for {dest_table}"
            )
        except Exception as e:
            self.__logger.error(
                f"An error has occurred during load process for {dest_table}. ", e
            )
            raise

    def __extract(self, conn_name, query, params: dict = None, source=None):
        self.__logger.info(f"Staring data extraction for {source}")
        try:
            query = query.as_string(conn_name)
            data = pd.read_sql(query, conn_name.connection, params=params)
            self.__logger.info(f"{len(data)} rows extracted from {source}")
            self.__logger.info("Data extraction successfully finished")
        except Exception as e:
            self.__logger.error("An error has occurred during data extraction. ", e)
            raise
        return data
