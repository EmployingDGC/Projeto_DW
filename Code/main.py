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

    # frame_ts_resultado_aluno = pd.read_csv(
    #     "../Datasets/TS_RESULTADO_ALUNO.csv",
    #     delimiter=";",
    #     usecols=[
    #         "ID_PROVA_BRASIL",
    #         "PROFICIENCIA_MT_SAEB",
    #         "PROFICIENCIA_LP_SAEB",
    #         "IN_SITUACAO_CENSO",
    #         "IN_PREENCHIMENTO",
    #         "IN_PROFICIENCIA"
    #     ]
    # )
    #
    # frame_ts_resultado_aluno["PROFICIENCIA_MT_SAEB"] = utl.convert_column_to_float64(
    #     column_data_frame=frame_ts_resultado_aluno["PROFICIENCIA_MT_SAEB"],
    #     default=DFLT.CD[0]
    # )
    #
    # frame_ts_resultado_aluno["PROFICIENCIA_LP_SAEB"] = utl.convert_column_to_float64(
    #     column_data_frame=frame_ts_resultado_aluno["PROFICIENCIA_LP_SAEB"],
    #     default=DFLT.CD[0]
    # )
    #
    # print(frame_ts_resultado_aluno)

    dimensions = ft.get_all_dimensions(conn_database)

    for frame in dimensions:
        print("*" + "-*" * 40)
        print(frame.dtypes)
        print()
        print(frame)
        print("*" + "-*" * 40)

    print(f"\nFinalizado com sucesso em {round(time() - time_exec)} segundos\n")
