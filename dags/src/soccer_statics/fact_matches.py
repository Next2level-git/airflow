import pandas as pd
from airflow.models import Variable
import logging
import uuid
from datetime import datetime

from next_connection.data_connection import PostgreSQLConnection
import src.soccer_statics.queries as queries


class Fact_Match:

    def __init__(self, data_extract):
        self.data = data_extract
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
        data["dim_teams"] = self.__extract(
            conn_name=nxdb, query=queries.dim_teams_extract, source="dim_teams"
        )
        data["dim_league"] = self.__extract(
            conn_name=nxdb, query=queries.dim_league_extract, source="dim_league"
        )
        data["fact_matches"] = self.__extract(
            conn_name=nxdb, query=queries.fact_matches_extract, source="fact_matches"
        )
        self.__logger.info("Start transform data")
        data_to_load = self.transform_data(data=data)
        if not data_to_load.empty:
            self.__logger.info("Start load new data")
            self.__load(
                conn=nxdb,
                data=data_to_load,
                schema="soccer",
                dest_table="fact_matches",
                truncate=False,
            )
        nxdb.destroy_connection()

    def transform_data(self, data):
        data_teams = data["dim_teams"][["uuid", "id_team"]]
        data_league = data["dim_league"][["uuid", "id_league"]]
        fact_matches = data["fact_matches"][["id_match"]]
        data_matches = self.data
        data_matches["id_match"] = data_matches["fixture"].apply(lambda x: x["id"])
        data_matches["date"] = data_matches["fixture"].apply(lambda x: x["date"])
        data_matches["id_league"] = data_matches["league"].apply(lambda x: x["id"])
        data_matches["id_home"] = data_matches["teams"].apply(lambda x: x["home"]["id"])
        data_matches["id_away"] = data_matches["teams"].apply(lambda x: x["away"]["id"])
        data_matches["home_goals"] = data_matches["goals"].apply(lambda x: x["home"])
        data_matches["away_goals"] = data_matches["goals"].apply(lambda x: x["away"])
        data_matches["home_goals_half_time"] = data_matches["score"].apply(
            lambda x: x["halftime"]["home"]
        )
        data_matches["away_goals_half_time"] = data_matches["score"].apply(
            lambda x: x["halftime"]["away"]
        )
        data_matches = data_matches[
            [
                "id_match",
                "date",
                "id_league",
                "id_home",
                "id_away",
                "home_goals",
                "away_goals",
                "home_goals_half_time",
                "away_goals_half_time",
            ]
        ]

        data_matches = data_matches.merge(
            data_teams, left_on="id_home", right_on="id_team", how="left"
        )
        data_matches["uuid_home_team"] = data_matches["uuid"]
        data_matches = data_matches.drop(columns=["id_home", "id_team", "uuid"])
        data_matches = data_matches.merge(
            data_teams, left_on="id_away", right_on="id_team", how="left"
        )
        data_matches["uuid_away_team"] = data_matches["uuid"]
        data_matches = data_matches.drop(columns=["id_away", "id_team", "uuid"])
        data_matches = data_matches.merge(data_league, on="id_league", how="left")
        data_matches["uuid_league"] = data_matches["uuid"]
        data_matches = data_matches.drop(columns=["id_league"])
        data_matches["date_match"] = pd.to_datetime(data_matches["date"]).dt.strftime(
            "%Y%m%d"
        )
        data_matches["date"] = pd.to_datetime(data_matches["date"])
        data_matches["hour_match"] = (
            data_matches["date"].dt.hour * 3600
            + data_matches["date"].dt.minute * 60
            + data_matches["date"].dt.second
        )
        data_matches = data_matches.drop(columns=["date"])

        data_merge_dim = data_matches.merge(
            fact_matches,
            on=["id_match"],
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
            result_append["id_match"] = result_append["id_match"].astype("Int64")
            result_append["home_goals"] = result_append["home_goals"].astype("Int64")
            result_append["away_goals"] = result_append["away_goals"].astype("Int64")
            result_append["home_goals_half_time"] = result_append[
                "home_goals_half_time"
            ].astype("Int64")
            result_append["away_goals_half_time"] = result_append[
                "away_goals_half_time"
            ].astype("Int64")
            result_append["date_match"] = result_append["date_match"].astype("Int64")
            result_append["hour_match"] = result_append["hour_match"].astype("Int64")
            result_append = result_append[
                [
                    "uuid",
                    "uuid_home_team",
                    "uuid_away_team",
                    "uuid_league",
                    "id_match",
                    "home_goals",
                    "away_goals",
                    "home_goals_half_time",
                    "away_goals_half_time",
                    "created_at",
                    "updated_at",
                    "date_match",
                    "hour_match",
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
