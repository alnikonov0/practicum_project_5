from pydantic import BaseModel
import datetime
import psycopg
from psycopg.rows import class_row
import json

PgConnect = psycopg.connect(
    host="localhost", dbname="de", user="jovyan", password="jovyan", port=5432
)


class UserRows(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime.datetime


class LoaderUsers:
    def __init__(self):
        self.pg_conn = PgConnect
        users = self.user_reader()
        self.insert_events(users)

    def user_reader(self):
        cursor = self.pg_conn.cursor(row_factory=class_row(UserRows))

        cursor.execute(
            """
            SELECT id, object_id, object_value, update_ts
            FROM stg.ordersystem_users
            WHERE id > COALESCE((SELECT workflow_settings->>'last_loaded_id' from dds.srv_wf_settings
                    where workflow_key = 'users_stg_to_dds_workflow')::INT, 0)
        """
        )

        rows = cursor.fetchall()

        cursor.close()

        result = []

        for i in rows:
            d = json.loads(i.object_value)
            result.append(
                {
                    "user_id": d.get("_id"),
                    "user_name": d.get("name"),
                    "user_login": d.get("login"),
                }
            )

        return result

    def insert_events(self, result):
        with self.pg_conn.cursor() as cur:
            cur.executemany(
                """
                    INSERT INTO dds.dm_users (user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                """,
                result,
            )

        self.pg_conn.commit()

        with self.pg_conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                    SELECT 'users_stg_to_dds_workflow', jsonb_build_object('last_loaded_id', coalesce(MAX(id),0)) as workflow_setting
                    FROM dds.dm_users
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET
                        workflow_settings = EXCLUDED.workflow_settings
                """
            )

        self.pg_conn.commit()

        cur.close()

        print("done")


load = LoaderUsers()
