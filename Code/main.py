from time import time

import pandas as pd

import CONEXAO as conn
import STAGES as stgs
import DIMENSOES as dms
import FATO as ft


if __name__ == '__main__':
    time_exec = time()
    time_initial = time()

    pd.set_option("display.max_columns", None)

    conn_database = conn.create_connection_postgre(
        server="10.0.0.102",
        database="postgres",
        username="postgres",
        password="itix.123",
        port=5432
    )

    # stgs.run(conn_database)
    #
    # print(f"\nStages carregadas em {round(time() - time_exec)} segundos\n")
    # time_exec = time()

    # dms.run(conn_database)
    #
    # print(f"\nDimensões carregadas em {round(time() - time_exec)} segundos\n")
    # time_exec = time()

    ft.run(conn_database)

    print(f"\nFato carregada em {round(time() - time_exec)} segundos\n")
    time_exec = time()

    print(ft.get_fato(conn_database))

    print(f"\nFato impressa em {round(time() - time_exec)} segundos\n")
    time_exec = time()

    print(f"\nFinalizado com sucesso em {round(time() - time_initial)} segundos\n")
