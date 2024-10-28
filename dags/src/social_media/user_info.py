import pandas as pd
import requests
from next_connection.data_connection import PostgreSQLConnection
import logging
from airflow.models import Variable
import src.social_media.queries as queries
import src.social_media.utils as utils
import json
import ast
import cairosvg
import base64


class User_info:

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
        data = dict()
        data["users"] = self.__extract(
            conn_name=nxdb, query=queries.user_querie, source="users data"
        )
        new_data, update_data = self.__transform_data(data=data)
        if not new_data.empty:
            self.__logger.info("Start load new data")
            self.__load(
                conn=nxdb,
                data=new_data,
                schema="social_media",
                dest_table="users",
                truncate=False,
            )

        if not update_data.empty:
            self.__logger.info("Start update old data")
            update_query = utils.cte_update_query(
                update_df=update_data.copy(),
                fields=utils.USERS_UPDATE_COLUMNS,
                update_query=queries.update_users_query.as_string(nxdb),
            )
            utils.postgresql_execute_query(
                user=self.user_db,
                password=self.password_db,
                database=self.db,
                host=self.host_db,
                query_execute=update_query.as_string(nxdb),
            )
            self.__logger.info(f"The update of {update_data.shape[0]} rows was made.")
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
                        "username",
                        "full_name",
                        "biography",
                        "category",
                        "media_count",
                        "hd_profile_pic_url_info"
                    ]
                ]
                load_data = pd.concat([df_new, load_data], ignore_index=True)
        return load_data

    def __transform_data(self, data):
        users_data = data["users"]
        new_data = self.__extract_api_information()
        new_data = new_data.rename(
            columns={
                "biography": "bio",
                "media_count": "publications",
                "latest_reel_media": "last_publication",
            }
        )
        new_data['url_image'] = new_data["hd_profile_pic_url_info"].apply(lambda x: x["url"])

        def download_and_convert_to_base64(url):
            try:
                response = requests.get(url)
                response.raise_for_status()

                if response.headers["Content-Type"] == "image/svg+xml":
                    svg_data = response.content
                    png_data = cairosvg.svg2png(bytestring=svg_data)
                else:
                    png_data = response.content
                base64_image = base64.b64encode(png_data).decode("utf-8")
                return base64_image
            except Exception as e:
                print(f"Error to downlad image: {e}")
                return None

        new_data["profile_picture"] = new_data["url_image"].apply(
            download_and_convert_to_base64
        )
        data_merge_dim = new_data.merge(
            users_data,
            on=["id"],
            how="outer",
            suffixes=["", "_crr"],
            indicator=True,
        )
        result_append = data_merge_dim[data_merge_dim["_merge"] == "left_only"]
        if not result_append.empty:
            result_append["publications"] = result_append["publications"].astype(
                "Int64"
            )
            result_append = result_append[
                [
                    "id",
                    "username",
                    "full_name",
                    "bio",
                    "category",
                    "publications",
                    "profile_picture"
                ]
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
                    "username",
                    "username_crr",
                    "full_name",
                    "full_name_crr",
                    "bio",
                    "bio_crr",
                    "category",
                    "category_crr",
                    "publications",
                    "publications_crr",
                    "profile_picture",
                    "profile_picture_crr",
                ]
            ].apply(find_different_columns, axis=1)
            result_update = result_update[result_update["check_update"] != "N"]
            result_update = result_update[utils.USERS_UPDATE_COLUMNS.keys()]

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
