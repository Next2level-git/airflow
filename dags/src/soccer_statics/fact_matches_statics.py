import pandas as pd
from airflow.models import Variable
import logging
import uuid
from datetime import datetime
import concurrent.futures
import requests

from next_connection.data_connection import PostgreSQLConnection
import src.soccer_statics.queries as queries


class Fact_Match_Statics:

    def __init__(self):
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
        data["fact_matches"] = self.__extract(
            conn_name=nxdb, query=queries.fact_matches_statistics_extract, source="fact_matches_statistics"
        )
        if not data["fact_matches"].empty:
            matches_to_search = data["fact_matches"]
            matches_to_search["id_match"] = matches_to_search["id_match"].astype("str")
            matches_to_request = matches_to_search["id_match"].tolist()
            results_list = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = executor.map(self.__send_get_request, matches_to_request)
                for result in results:
                    if result is not None:
                        results_list.append(result)

        data['data_matches_statistics'] = pd.DataFrame(results_list)
        self.__logger.info("Start transform data")
        data_to_load = self.transform_data(data=data)
        if not data_to_load.empty:
            self.__logger.info("Start load new data")
            self.__load(
                conn=nxdb,
                data=data_to_load,
                schema="soccer",
                dest_table="fact_matches_statistics",
                truncate=False,
            )
        nxdb.destroy_connection()

    def __send_get_request(self, matches_to_request):
        try:
            url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
            querystring = {"fixture": f"{matches_to_request}"}
            headers = {
                "x-rapidapi-key": "5ada26f90cmsh42c7290dd83d724p1f721cjsn4b0c4acc71b9",
                "x-rapidapi-host": "api-football-v1.p.rapidapi.com"
            }
            self.__logger.info(
                f"Request to match {matches_to_request}"
            )
            response = requests.get(url, headers=headers, params=querystring)
            if response.status_code == 200:
                json_data = response.json()
                data_with_token = {"id_match": matches_to_request, "response": json_data}
                return data_with_token
            else:
                return None
        except Exception as e:
            self.__logger.error("An error has occurred during data credential status. ", e)
            return None

    def transform_data(self, data):
        data_teams = data["dim_teams"][["uuid", "id_team"]]
        fact_matches = data["fact_matches"][["id_match","uuid"]]
        data_matches_statistics = data["data_matches_statistics"]
        data_matches_statistics['id_team'] = data_matches_statistics['response'].apply(lambda x: x['team']['id'])
        def extract_statistics(stats):
            return {stat['type']: stat['value'] for stat in stats['statistics']}
        stats_df = data_matches_statistics['response'].apply(extract_statistics).apply(pd.Series)
        data_matches_statistics = pd.concat([data_matches_statistics[['id_match','id_team']], stats_df], axis=1)
        data_matches_statistics = data_matches_statistics[["Shots on Goal",
                                                           "Shots off Goal",
                                                           "Total Shots",
                                                           "Blocked Shots",
                                                           "Shots insidebox",
                                                           "Shots outsidebox",
                                                           "Fouls",
                                                           "Corner Kicks",
                                                           "Offsides",
                                                           "Ball Possession",
                                                           "Yellow Cards",
                                                           "Red Cards",
                                                           "Goalkeeper Saves",
                                                           "Total passes",
                                                           "Passes accurate",
                                                           "id_team",
                                                           "c",]]
        data_matches_statistics = data_matches_statistics.rename(columns={"Shots on Goal": "shots_on_goal",
                                                                          "Shots off Goal": "shots_off_goal",
                                                                          "Total Shots": "total_shots",
                                                                          "Blocked Shots": "blocked_shots",
                                                                          "Shots insidebox": "shots_insidebox",
                                                                          "Shots outsidebox": "shots_outsidebox",
                                                                          "Fouls": "fouls",
                                                                          "Corner Kicks": "corner_kics",
                                                                          "Offsides": "offsides",
                                                                          "Ball Possession": "ball_possesion",
                                                                          "Yellow Cards": "yellow_cards",
                                                                          "Red Cards": "red_cards",
                                                                          "Goalkeeper Saves": "goalkeeper_saves",
                                                                          "Total passes": "total_passes",
                                                                          "Passes accurate": "passes_accurate"})
        data_matches_statistics = data_matches_statistics.merge(data_teams,
                                                                on='id_team',
                                                                how='left')
        data_matches_statistics = data_matches_statistics.rename(columns={"uuid":"uuid_team"})
        data_matches_statistics = data_matches_statistics.merge(fact_matches,
                                                                on='fact_matches',
                                                                how='left')
        data_matches_statistics = data_matches_statistics.rename(columns={"uuid":"uuid_match"})
        data_matches_statistics['ball_possesion'] = data_matches_statistics['ball_possesion'].str.replace('%', '').astype(float) / 100
        if not data_matches_statistics.empty:
            data_matches_statistics["uuid"] = data_matches_statistics.apply(lambda _: uuid.uuid4(), axis=1)
            data_matches_statistics["created_at"] = pd.to_datetime(
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            )
            data_matches_statistics["updated_at"] = pd.to_datetime(
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            )
            data_matches_statistics["shots_on_goal"] = data_matches_statistics["shots_on_goal"].astype("Int64")
            data_matches_statistics["shots_off_goal"] = data_matches_statistics["shots_off_goal"].astype("Int64")
            data_matches_statistics["total_shots"] = data_matches_statistics["total_shots"].astype("Int64")
            data_matches_statistics["blocked_shots"] = data_matches_statistics[
                "blocked_shots"
            ].astype("Int64")
            data_matches_statistics["shots_insidebox"] = data_matches_statistics[
                "shots_insidebox"
            ].astype("Int64")
            data_matches_statistics["shots_outsidebox"] = data_matches_statistics["shots_outsidebox"].astype("Int64")
            data_matches_statistics["fouls"] = data_matches_statistics["fouls"].astype("Int64")
            data_matches_statistics["corner_kics"] = data_matches_statistics["corner_kics"].astype("Int64")
            data_matches_statistics["offsides"] = data_matches_statistics["offsides"].astype("Int64")
            data_matches_statistics["yellow_cards"] = data_matches_statistics["yellow_cards"].astype("Int64")
            data_matches_statistics["red_cards"] = data_matches_statistics["red_cards"].astype("Int64")
            data_matches_statistics["goalkeeper_saves"] = data_matches_statistics["goalkeeper_saves"].astype("Int64")
            data_matches_statistics["total_passes"] = data_matches_statistics["total_passes"].astype("Int64")
            data_matches_statistics["passes_accurate"] = data_matches_statistics["passes_accurate"].astype("Int64")

        return data_matches_statistics

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
