import pandas as pd
from airflow.models import Variable
import logging
import uuid
from datetime import datetime
from next_connection.data_connection import PostgreSQLConnection

class Dim_Teams:

    def __init__(self, data_teams):
        self.data_teams = data_teams
        self.host_db = Variable.get("HOST_DB_NEXT")
        self.user_db = Variable.get("USER_DB_NEXT")
        self.password_db = Variable.get("PASSWORD_DB_NEXT")
        self.db = Variable.get("DB_NEXT")
        log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        self.__logger = logging.getLogger(__name__)

    def run(self):
        self.__logger.info("Start transform data")
        data_to_load = self.transform_data()
        nxdb = PostgreSQLConnection(
            user=self.user_db,
            password=self.password_db,
            host_name=self.host_db,
            database=self.db,
        )
        nxdb.create_connection()
        if not data_to_load.empty:
            self.__load(
                conn=nxdb,
                data=data_to_load,
                schema="soccer",
                dest_table="dim_teams",
                truncate=False,
            )


    def transform_data(self):
        data = self.data_teams
        df_home_team = pd.DataFrame(data['teams'].apply(lambda x: x['home']).tolist())
        df_home_team = df_home_team[['id', 'name']]
        df_away_team = pd.DataFrame(data['teams'].apply(lambda x: x['away']).tolist())
        df_away_team = df_home_team[['id', 'name']]
        data = pd.concat([df_away_team,df_home_team], ignore_index=True)
        data = data.reset_index(drop=True)
        data = data.drop_duplicates(subset=['id'], keep='first')
        if not data.empty:
            data['uuid'] = data.apply(lambda _: uuid.uuid4(), axis=1)
            data['created_at'] = pd.to_datetime(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
            data['updated_at'] = pd.to_datetime(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
            data['id_team'] = data['id'].astype("Int64")
            data['name_team'] = data['name']
            data = data[['uuid','id_team','name_team','created_at','updated_at']]
        return data

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