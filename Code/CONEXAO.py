import sqlalchemy as sa
# import pandas as pd


def create_connection_postgre(server: str,
                              database: str,
                              username: str,
                              password: str,
                              port: int) -> sa.engine.mock.MockConnection:
    conn = f'postgresql+psycopg2://{username}:{password}@{server}:{port}/{database}'
    return sa.create_engine(conn)
