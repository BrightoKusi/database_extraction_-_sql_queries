import psycopg2


def my_postgres_conn(host, user, password, database):
    conn = psycopg2.connect(
            host = host
            , user = user
            , password = password
            , database = database)
    return conn
    