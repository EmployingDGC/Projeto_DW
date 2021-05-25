from time import time

from sqlalchemy.engine.mock import MockConnection

import pandas as pd

import CONEXAO as conn
import STAGES as stgs
import DIMENSOES as dms


if __name__ == '__main__':
    time_exec = time()

    db = conn.create_connection_postgre(
        server="localhost",
        database="postgres",
        username="postgres",
        password="itix.123",
        port=5432
    )



    print(f"Finalizado com sucesso em {round(time() - time_exec)} segundos")
