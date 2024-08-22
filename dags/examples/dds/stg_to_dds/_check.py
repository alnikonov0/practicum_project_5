from pydantic import BaseModel
import datetime
from typing import List, Any, Optional
import psycopg
from psycopg.rows import class_row
import json

# Подключение к базе данных
PgConnect = psycopg.connect(
    host="localhost",
    dbname="de",
    user="jovyan",
    password="jovyan",
    port="15432"
)

class UserRows(BaseModel):  
    id: int
    object_id: str
    object_value: str
    update_ts: datetime.datetime

class UserRowsFinal(BaseModel):  
    user_id: str
    user_name: str
    user_login: str

class LoaderUsers:
    def __init__(self):
        self.pg_conn = PgConnect
        users = self.user_reader()
        self.user_loader(users)

    def user_reader(self) -> List[UserRowsFinal]:
        # Создание курсора с row_factory
        cursor = self.pg_conn.cursor(row_factory=class_row(UserRows))

        # Выполнение запроса
        cursor.execute('''
            SELECT id, object_id, object_value, update_ts
            FROM stg.ordersystem_users
        ''')

        # Получение всех строк результата запроса
        rows = cursor.fetchall()

        cursor.close()

        result = []

        for i in rows:
            d = json.loads(i.object_value)
            user_row_final = UserRowsFinal(
                user_id=d.get('_id'),
                user_name=d.get('name'),
                user_login=d.get('login')
            )
            result.append(user_row_final)
            # result.append((d.get('_id'), d.get('name'), d.get('login')))


        return result

    def user_loader(self, users: List[UserRowsFinal]):
        for user in users:
            self.insert_events(user)

    def insert_events(self, user: UserRowsFinal):
        with self.pg_conn.cursor() as cur:
            cur.execute(
                '''
                    INSERT INTO dds.dm_users (user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                ''',
                {
                    'user_id': user.user_id,
                    'user_name': user.user_name,
                    'user_login': user.user_login
                }
            )
        self.pg_conn.commit()

        print('done')

# Запуск загрузчика
load = LoaderUsers()

# Вставляем данные в другую таблицу
cursor.executemany("INSERT INTO another_table VALUES (?, ?, ?)", rows)