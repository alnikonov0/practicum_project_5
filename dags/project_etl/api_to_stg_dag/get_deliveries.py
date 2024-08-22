import requests
import datetime
import sys

sys.path.append("/lessons/dags")

from project_etl.con import get_pg_connection


wf_key = "get_deliveries"
limit = 50
offset = 0

from_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime(
    "%Y-%m-%d %H:%M:%S"
)
to_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_limit(wf_key: str) -> list:
    connection = get_pg_connection()

    with connection.cursor() as cur:
        cur.execute(
            f"""
                    select workflow_settings ->> 'limit' as limit
                        from stg.srv_wf_settings
                    where workflow_key = 'get_deliveries'
                """
        )
        res = cur.fetchone()

    connection.close()

    return res[0] if res else 50


def get_offset(wf_key: str) -> list:
    connection = get_pg_connection()

    with connection.cursor() as cur:
        cur.execute(
            f"""
                    select workflow_settings ->> 'offset' as offset
                        from stg.srv_wf_settings
                    where workflow_key = 'get_deliveries'
                """
        )
        res = cur.fetchone()

    connection.close()

    return int(res[0]) if res else 0


def get_api_data(offset):
    url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries"
    params = {
        "sort_field": "_id",
        "sort_direction": "asc",
        "limit": limit,
        "offset": offset,
        "from": from_date,
        "to": to_date,
    }

    headers = {
        "X-Nickname": "betelgausepf",
        "X-Cohort": "27",
        "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def insert(data):
    connection = get_pg_connection()

    with connection.cursor() as cur:
        cur.executemany(
            """INSERT INTO stg.api_deliveries (order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum)
                    VALUES (%(order_id)s, %(order_ts)s, %(delivery_id)s, %(courier_id)s, %(address)s, %(delivery_ts)s, %(rate)s, %(sum)s, %(tip_sum)s)
                    """,
            data,
        )

    connection.commit()

    connection.close()

    print("data insert done")


def update_workflow(offset):
    connection = get_pg_connection()

    with connection.cursor() as cur:
        cur.execute(
            f"""
                        insert into stg.srv_wf_settings (workflow_key, workflow_settings)
                        values ('{wf_key}', json_build_object('limit', 50, 'offset', '{offset}'))
                        ON CONFLICT (workflow_key) DO UPDATE
                        SET workflow_settings = EXCLUDED.workflow_settings
                        """
        )
    connection.commit()

    connection.close()

    print("workflow update done")


def loader_deliveries():
    counter = 0
    offset = get_offset(wf_key)
    print(f"last load id = {offset}. No rows to update.")
    while True:
        data = get_api_data(offset)
        if not data:
            break
        insert(data)

        if len(data) < 50:
            offset += len(data)
        else:
            offset += limit
        update_workflow(offset)
        counter += 1
        print(f"step {counter}")


loader_deliveries()
