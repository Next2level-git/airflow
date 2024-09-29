import pandas as pd
from psycopg2 import sql, connect

FIELD_TO_CHAR = ["String", "Date"]
MAX_DATAFRAME_SIZE = 15000

def cte_update_query(update_df: pd.DataFrame, update_query: str, fields: dict) -> sql.SQL:
    head_cte_update = "WITH cte (" + ",".join(fields.keys()) + ") AS ("
    update_df["select_field"] = "SELECT"
    update_df["union_field"] = "UNION ALL"
    update_df.iloc[-1, update_df.columns.get_loc("union_field")] = ""  # Última fila sin UNION ALL

    for key, value in fields.items():
        if value in FIELD_TO_CHAR:
            update_df[key] = update_df[key].apply(lambda x: f"'{x}'" if pd.notnull(x) else "NULL")
    update_df.replace("'None'", "NULL", inplace=True)
    update_df["body_ctr_update"] = update_df[fields.keys()].astype(str).apply(",".join, axis=1)
    update_df["body_ctr_update"] = (
        update_df[["select_field", "body_ctr_update", "union_field"]]
        .astype(str)
        .apply(" ".join, axis=1)
    )
    cte_query = head_cte_update + " ".join(update_df["body_ctr_update"].tolist()) + ")"
    cte_query = cte_query.replace("None,", "NULL,").replace("nan,", "NULL,").replace("<NA>,", "NULL::INT,")
    final_query = cte_query + " " + update_query
    return sql.SQL(final_query)


def postgresql_execute_query(user, password, database, host, query_execute, port="5432"):
    conn = connect(database=database, user=user, password=password, host=host, port=port)

    conn.autocommit = True
    cursor = conn.cursor()
    try:
        cursor.execute(query_execute)
        conn.commit()
    except Exception as ex:
        print("Exception while executing the postgresql query {}".format(ex))
        raise
    finally:
        conn.close()

DIM_TEAMS_UPDATE_COLUMNS = {
    "uuid": "String",
    "name_team": "String",
    "team_logo": "String",
    "updated_at": "Date",
}

DIM_LEAGUE_UPDATE_COLUMNS = {
    "uuid": "String",
    "country": "String",
    "name_league": "String",
    "league_logo": "String",
    "country_flag": "String",
    "updated_at": "Date",
}