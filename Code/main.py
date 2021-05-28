from time import time

import pandas as pd

import CONEXAO as conn
import STAGES as stgs
import DIMENSOES as dms
import FATO as ft

import DEFAULTS_VALUES as DFLT
import utilities as utl


if __name__ == '__main__':
    time_exec = time()

    pd.set_option("display.max_columns", None)

    conn_database = conn.create_connection_postgre(
        server="10.0.0.102",
        database="postgres",
        username="postgres",
        password="itix.123",
        port=5432
    )

    # stgs.run(conn_database)

    # dms.run(conn_database)

    # ft.run(conn_database)

    print(f"\nFinalizado com sucesso em {round(time() - time_exec)} segundos\n")
