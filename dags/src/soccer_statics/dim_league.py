import pandas as pd
from airflow.models import Variable
import logging
import uuid
from datetime import datetime
import requests
import base64
from io import BytesIO
import cairosvg

from next_connection.data_connection import PostgreSQLConnection
import src.soccer_statics.queries as queries
import src.soccer_statics.utils as utils


class Dim_League:

    def __init__(self, data_league):
        self.data_league = data_league
        self.host_db = Variable.get("HOST_DB_NEXT")
        self.user_db = Variable.get("USER_DB_NEXT")
        self.password_db = Variable.get("PASSWORD_DB_NEXT")
        self.db = Variable.get("DB_NEXT")
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
        data = dict()
        data["dim_league"] = self.__extract(
            conn_name=nxdb, query=queries.dim_league_extract, source="dim_league"
        )
        self.__logger.info("Start transform data")
        data_to_load, data_to_update = self.transform_data(data=data)
        if not data_to_load.empty:
            self.__logger.info("Start load new data")
            self.__load(
                conn=nxdb,
                data=data_to_load,
                schema="soccer",
                dest_table="dim_league",
                truncate=False,
            )

        if not data_to_update.empty:
            self.__logger.info("Start update old data")
            update_query = utils.cte_update_query(
                update_df=data_to_update.copy(),
                fields=utils.DIM_LEAGUE_UPDATE_COLUMNS,
                update_query=queries.update_dim_league_query.as_string(nxdb),
            )
            utils.postgresql_execute_query(
                user=self.user_db,
                password=self.password_db,
                database=self.db,
                host=self.host_db,
                query_execute=update_query.as_string(nxdb),
            )
            self.__logger.info(
                f"The update of {data_to_update.shape[0]} rows was made."
            )

    def transform_data(self, data):
        data_current_dim = data["dim_league"]
        data_league = self.data_league
        data_league = pd.json_normalize(data_league['league'])
        data_league = data_league[['id','name','country','logo','flag']]
        data_league = data_league.drop_duplicates(subset=["id"], keep="first")
        data_league = data_league.reset_index(drop=True)
        def download_and_convert_to_base64(url):
            try:
                response = requests.get(url)
                response.raise_for_status()

                if response.headers['Content-Type'] == 'image/svg+xml':
                    svg_data = response.content
                    png_data = cairosvg.svg2png(bytestring=svg_data)
                else:
                    png_data = response.content
                base64_image = base64.b64encode(png_data).decode('utf-8')
                return base64_image
            except Exception as e:
                print(f"Error to downlad image: {e}")
                return None
        data_league["league_logo"] = data_league["logo"].apply(download_and_convert_to_base64)
        data_league["country_flag"] = data_league["flag"].apply(download_and_convert_to_base64)
        data_league = data_league.rename(columns={"id": "id_league",
                                                "name": "name_league"})
        data_merge_dim = data_league.merge(
            data_current_dim,
            on=["id_league"],
            how="outer",
            suffixes=["", "_crr"],
            indicator=True,
        )
        result_append = data_merge_dim[data_merge_dim["_merge"] == "left_only"]
        if not result_append.empty:
            result_append["uuid"] = result_append.apply(lambda _: uuid.uuid4(), axis=1)
            result_append["created_at"] = pd.to_datetime(
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            )
            result_append["updated_at"] = pd.to_datetime(
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            )
            result_append["id_league"] = result_append["id_league"].astype("Int64")
            result_append = result_append[
                ["uuid", "id_league", "country","name_league","league_logo","country_flag", "created_at", "updated_at"]
            ]
        result_update = data_merge_dim[data_merge_dim["_merge"] == "both"]
        result_update = result_update.where(result_update.notnull(), None)
        if not result_update.empty:

            def find_different_columns(row):
                different_columns = []
                for i in range(0, len(row), 2):
                    if row.iloc[i] != row.iloc[i + 1]:
                        different_columns.append(row.index[i])
                return different_columns if different_columns else "N"

            result_update["check_update"] = result_update[
                [
                    "country",
                    "country_crr",
                    "name_league",
                    "name_league_crr",
                    "league_logo",
                    "league_logo_crr",
                    "country_flag",
                    "country_flag_crr"
                ]
            ].apply(find_different_columns, axis=1)
            result_update = result_update[result_update["check_update"] != "N"]
            result_update = result_update[utils.DIM_LEAGUE_UPDATE_COLUMNS.keys()]
            result_update["updated_at"] = pd.to_datetime(
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            )
        return result_append, result_update

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
